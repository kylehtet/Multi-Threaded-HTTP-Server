/*
 * Phase 4: Thread pool (fixes the overhead we measured in Phase 3)
 *
 * What we learned from benchmarking Phase 3: spawning a brand new OS thread
 * for every single connection has real overhead (stack allocation, OS
 * scheduling setup). That's why Phase 3's average latency was ~4x worse
 * than Phase 2, even though throughput and error rate improved.
 *
 * The fix: create a FIXED pool of worker threads once, at startup. Instead
 * of spawning a new thread per request, the main thread just drops each new
 * connection into a thread-safe queue, and whichever worker is free picks
 * it up. No repeated thread-creation cost per request.
 *
 * New concepts introduced:
 * - A bounded queue (ring buffer) shared between the main thread and workers
 * - A mutex to protect the queue from concurrent access
 * - Condition variables (pthread_cond_t) so worker threads can sleep until
 *   there's actually work, instead of busy-waiting and burning CPU
 *
 * Build:  gcc -o server_phase4 server_phase4.c -lpthread
 * Run:    ./server_phase4
 * Benchmark the same way as Phase 2/3 and compare all three numbers.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <pthread.h>

#define PORT 8888
#define BUFFER_SIZE 4096
#define WWW_ROOT "www"
#define THREAD_POOL_SIZE 64   // number of worker threads created at startup
#define QUEUE_CAPACITY 256   // max pending connections waiting for a free worker

// ---------- Thread-safe connection queue ----------
// This is a simple bounded ring buffer of client file descriptors, shared
// between the main (accept) thread and the pool of worker threads.
typedef struct {
    int items[QUEUE_CAPACITY];
    int front;
    int rear;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty; // workers wait on this until there's a job
    pthread_cond_t not_full;  // main thread waits on this if the queue is full
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

    // If the queue is full, wait until a worker frees up space.
    while (q->count == QUEUE_CAPACITY) {
        pthread_cond_wait(&q->not_full, &q->lock);
    }

    q->items[q->rear] = client_fd;
    q->rear = (q->rear + 1) % QUEUE_CAPACITY;
    q->count++;

    pthread_cond_signal(&q->not_empty); // wake up one sleeping worker
    pthread_mutex_unlock(&q->lock);
}

int queue_pop(ConnectionQueue *q) {
    pthread_mutex_lock(&q->lock);

    // If there's nothing to do, sleep until queue_push() signals us.
    while (q->count == 0) {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }

    int client_fd = q->items[q->front];
    q->front = (q->front + 1) % QUEUE_CAPACITY;
    q->count--;

    pthread_cond_signal(&q->not_full); // wake up main thread if it was waiting to push
    pthread_mutex_unlock(&q->lock);

    return client_fd;
}

ConnectionQueue conn_queue;

// ---------- Logging (same as Phase 3) ----------
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

// ---------- HTTP handling (same logic as Phase 2/3) ----------
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

void send_response(int client_fd, const char *status, const char *content_type,
                    const char *body, size_t body_len) {
    char header[512];
    int header_len = snprintf(header, sizeof(header),
        "HTTP/1.1 %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n",
        status, content_type, body_len);

    write(client_fd, header, header_len);
    write(client_fd, body, body_len);
}

void send_404(int client_fd) {
    const char *body = "<html><body><h1>404 Not Found</h1></body></html>";
    send_response(client_fd, "404 Not Found", "text/html", body, strlen(body));
}

void serve_file(int client_fd, const char *requested_path) {
    char full_path[1200];

    if (strcmp(requested_path, "/") == 0) {
        requested_path = "/index.html";
    }

    snprintf(full_path, sizeof(full_path), "%s%s", WWW_ROOT, requested_path);

    FILE *fp = fopen(full_path, "rb");
    if (!fp) {
        safe_log("[worker %lu] File not found: %s\n", pthread_self(), full_path);
        send_404(client_fd);
        return;
    }

    struct stat st;
    stat(full_path, &st);
    long file_size = st.st_size;

    char *body = malloc(file_size);
    fread(body, 1, file_size, fp);
    fclose(fp);

    send_response(client_fd, "200 OK", get_content_type(full_path), body, file_size);

    free(body);
}

void handle_connection(int client_fd) {
    char buffer[BUFFER_SIZE];
    memset(buffer, 0, BUFFER_SIZE);
    ssize_t bytes_read = read(client_fd, buffer, BUFFER_SIZE - 1);

    if (bytes_read > 0) {
        char path[1024];
        if (parse_request_path(buffer, path, sizeof(path))) {
            serve_file(client_fd, path);
        } else {
            send_response(client_fd, "400 Bad Request", "text/plain",
                          "Bad Request", strlen("Bad Request"));
        }
    }

    close(client_fd);
}

// ---------- Worker thread ----------
// Each worker just loops forever: pull a connection off the queue, handle
// it, repeat. If the queue is empty, queue_pop() puts this thread to sleep
// (via the condition variable) instead of spinning and wasting CPU.
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

    // Spin up the fixed pool of worker threads ONCE, at startup.
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

    printf("Server (thread pool, %d workers) listening on http://localhost:%d\n",
           THREAD_POOL_SIZE, PORT);
    printf("Serving files from ./%s/\n\n", WWW_ROOT);

    // The main thread's ONLY job now is accepting connections and pushing
    // them into the queue -- it never does any request handling itself.
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
