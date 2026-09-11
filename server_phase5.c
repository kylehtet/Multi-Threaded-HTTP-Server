/*
 * Phase 5: HTTP Keep-Alive (testing the TCP handshake overhead theory)
 *
 * In Phase 2-4, every single request closes the TCP connection afterward
 * ("Connection: close"). That means EVERY request pays the cost of a fresh
 * TCP handshake (SYN, SYN-ACK, ACK) before the actual HTTP exchange even
 * starts. We suspect this fixed per-request cost is why latency plateaued
 * around ~5ms regardless of thread count in Phase 3/4.
 *
 * The fix: support "Connection: keep-alive". After sending a response, DON'T
 * close the socket -- loop back and read another request on the SAME
 * connection, as long as the client wants to keep talking. This means one
 * TCP handshake can serve many requests instead of just one.
 *
 * New concepts introduced:
 * - Looping read/response on a single connection instead of one-shot
 * - Detecting whether the client requested keep-alive vs. close
 * - A per-connection timeout so idle keep-alive connections don't hang
 *   forever and leak a worker thread
 *
 * Build:  gcc -o server_phase5 server_phase5.c -lpthread
 * Run:    ./server_phase5
 *
 * IMPORTANT for benchmarking: wrk defaults to using keep-alive connections
 * automatically when the server supports it, so re-running the exact same
 * `wrk -t4 -c50 -d10s` command against this version is a fair, direct test
 * of the handshake-overhead theory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <pthread.h>

#define PORT 8888
#define BUFFER_SIZE 4096
#define WWW_ROOT "www"
#define THREAD_POOL_SIZE 64
#define QUEUE_CAPACITY 256
#define KEEPALIVE_TIMEOUT_SEC 5   // drop idle keep-alive connections after this long
#define MAX_REQUESTS_PER_CONN 1000 // safety cap so one connection can't loop forever

// ---------- Thread-safe connection queue (same as Phase 4) ----------
typedef struct {
    int items[QUEUE_CAPACITY];
    int front;
    int rear;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} ConnectionQueue;

void queue_init(ConnectionQueue *q) {
    q->front = 0;
    q->rear = 0;
    q->count = 0;
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

void queue_push(ConnectionQueue *q, int client_fd) {
    pthread_mutex_lock(&q->lock);
    while (q->count == QUEUE_CAPACITY) {
        pthread_cond_wait(&q->not_full, &q->lock);
    }
    q->items[q->rear] = client_fd;
    q->rear = (q->rear + 1) % QUEUE_CAPACITY;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

int queue_pop(ConnectionQueue *q) {
    pthread_mutex_lock(&q->lock);
    while (q->count == 0) {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    int client_fd = q->items[q->front];
    q->front = (q->front + 1) % QUEUE_CAPACITY;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);
    return client_fd;
}

ConnectionQueue conn_queue;

// ---------- Logging ----------
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

void safe_log(const char *fmt, ...) {
    va_list args;
    pthread_mutex_lock(&log_mutex);
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
    fflush(stdout);
    pthread_mutex_unlock(&log_mutex);
}

// ---------- HTTP helpers ----------
const char *get_content_type(const char *path) {
    const char *ext = strrchr(path, '.');
    if (!ext) return "application/octet-stream";

    if (strcmp(ext, ".html") == 0) return "text/html";
    if (strcmp(ext, ".css") == 0)  return "text/css";
    if (strcmp(ext, ".js") == 0)   return "application/javascript";
    if (strcmp(ext, ".png") == 0)  return "image/png";
    if (strcmp(ext, ".jpg") == 0)  return "image/jpeg";
    if (strcmp(ext, ".txt") == 0)  return "text/plain";

    return "application/octet-stream";
}

int parse_request_path(const char *request, char *out_path, size_t out_size) {
    char method[16];
    char path[1024];

    if (sscanf(request, "%15s %1023s", method, path) != 2) {
        return 0;
    }
    if (strcmp(method, "GET") != 0) {
        return 0;
    }

    strncpy(out_path, path, out_size - 1);
    out_path[out_size - 1] = '\0';
    return 1;
}

// Checks the raw request text for "Connection: close" (case-insensitive-ish,
// good enough for our purposes). If absent, we treat it as keep-alive,
// which matches HTTP/1.1's default behavior.
int client_wants_close(const char *request) {
    return strcasestr(request, "Connection: close") != NULL;
}

// Now takes a `keep_alive` flag so we can set the right response header.
// Sends header + body as ONE write() call instead of two separate writes.
// Splitting them across two writes lets Nagle's algorithm and the client's
// delayed-ACK behavior interact badly, adding tens to hundreds of ms of
// latency per request on keep-alive connections. Combining into one buffer
// (or setting TCP_NODELAY, done at accept time below) avoids this entirely.
void send_response(int client_fd, const char *status, const char *content_type,
                    const char *body, size_t body_len, int keep_alive) {
    char header[512];
    int header_len = snprintf(header, sizeof(header),
        "HTTP/1.1 %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: %s\r\n"
        "\r\n",
        status, content_type, body_len, keep_alive ? "keep-alive" : "close");

    // Combine header + body into one contiguous buffer for a single write().
    size_t total_len = header_len + body_len;
    char *full_response = malloc(total_len);
    memcpy(full_response, header, header_len);
    memcpy(full_response + header_len, body, body_len);

    write(client_fd, full_response, total_len);

    free(full_response);
}

void send_404(int client_fd, int keep_alive) {
    const char *body = "<html><body><h1>404 Not Found</h1></body></html>";
    send_response(client_fd, "404 Not Found", "text/html", body, strlen(body), keep_alive);
}

void serve_file(int client_fd, const char *requested_path, int keep_alive) {
    char full_path[1200];

    if (strcmp(requested_path, "/") == 0) {
        requested_path = "/index.html";
    }

    snprintf(full_path, sizeof(full_path), "%s%s", WWW_ROOT, requested_path);

    FILE *fp = fopen(full_path, "rb");
    if (!fp) {
        send_404(client_fd, keep_alive);
        return;
    }

    struct stat st;
    stat(full_path, &st);
    long file_size = st.st_size;

    char *body = malloc(file_size);
    fread(body, 1, file_size, fp);
    fclose(fp);

    send_response(client_fd, "200 OK", get_content_type(full_path), body, file_size, keep_alive);

    free(body);
}

// THE KEY CHANGE: this now loops, handling MULTIPLE requests on the same
// connection, instead of reading one request and closing.
void handle_connection(int client_fd) {
    char buffer[BUFFER_SIZE];

    // Disable Nagle's algorithm on this connection. Without this, small
    // writes can be delayed by the OS waiting to batch them, which
    // interacts badly with delayed ACKs on the client side -- a classic
    // source of tens-to-hundreds of ms of hidden latency on keep-alive
    // connections. Belt-and-suspenders alongside combining writes above.
    int nodelay = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    // Set a read timeout so an idle keep-alive connection doesn't hold a
    // worker thread hostage forever if the client just goes quiet.
    struct timeval tv;
    tv.tv_sec = KEEPALIVE_TIMEOUT_SEC;
    tv.tv_usec = 0;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    for (int req_count = 0; req_count < MAX_REQUESTS_PER_CONN; req_count++) {
        memset(buffer, 0, BUFFER_SIZE);
        ssize_t bytes_read = read(client_fd, buffer, BUFFER_SIZE - 1);

        if (bytes_read <= 0) {
            // Client closed the connection, or we hit the idle timeout.
            break;
        }

        int keep_alive = !client_wants_close(buffer);

        char path[1024];
        if (parse_request_path(buffer, path, sizeof(path))) {
            serve_file(client_fd, path, keep_alive);
        } else {
            send_response(client_fd, "400 Bad Request", "text/plain",
                          "Bad Request", strlen("Bad Request"), keep_alive);
        }

        if (!keep_alive) {
            break; // client asked us to close after this response
        }
        // otherwise: loop back and read the NEXT request on this same connection
    }

    close(client_fd);
}

void *worker_thread(void *arg) {
    int worker_id = *(int *)arg;
    free(arg);

    safe_log("[worker %d] ready\n", worker_id);

    while (1) {
        int client_fd = queue_pop(&conn_queue);
        handle_connection(client_fd);
    }

    return NULL;
}

int main() {
    int server_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);

    queue_init(&conn_queue);

    pthread_t workers[THREAD_POOL_SIZE];
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        int *id = malloc(sizeof(int));
        *id = i;
        pthread_create(&workers[i], NULL, worker_thread, id);
        pthread_detach(workers[i]);
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 128) < 0) {
        perror("listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Server (thread pool + keep-alive, %d workers) listening on http://localhost:%d\n",
           THREAD_POOL_SIZE, PORT);
    printf("Serving files from ./%s/\n\n", WWW_ROOT);

    while (1) {
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("accept failed");
            continue;
        }

        queue_push(&conn_queue, client_fd);
    }

    close(server_fd);
    return 0;
}
