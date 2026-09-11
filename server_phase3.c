/*
 * Phase 3: Thread-per-connection concurrency
 *
 * The problem with Phase 1/2: the server handles ONE connection at a time.
 * If client A's request takes a while (slow network, big file, etc.), client B
 * has to wait for A to finish before the server even starts on B's request.
 *
 * Fix: spawn a new thread for every accepted connection. Each thread handles
 * its own request/response independently, so slow/many clients don't block
 * each other.
 *
 * New concepts introduced:
 * - pthread_create() to spawn a worker thread per connection
 * - pthread_detach() so we don't have to manually pthread_join() every thread
 *   (detached threads clean up their own resources when they finish)
 * - A mutex around printf() logging, since multiple threads writing to
 *   stdout at the same time can interleave garbled output otherwise
 *
 * Build:  gcc -o server_phase3 server_phase3.c -lpthread
 * Run:    ./server_phase3
 * Test:   curl http://localhost:8888/
 *
 * To actually SEE the concurrency improvement, benchmark this against
 * server_phase2 using `wrk` or `ab` under concurrent load -- see the
 * benchmarking instructions at the bottom of this file.
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

// Mutex to protect stdout so log lines from different threads don't interleave
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
        safe_log("[thread %lu] File not found: %s\n", pthread_self(), full_path);
        send_404(client_fd);
        return;
    }

    struct stat st;
    stat(full_path, &st);
    long file_size = st.st_size;

    char *body = malloc(file_size);
    fread(body, 1, file_size, fp);
    fclose(fp);

    safe_log("[thread %lu] Serving %s (%ld bytes)\n", pthread_self(), full_path, file_size);
    send_response(client_fd, "200 OK", get_content_type(full_path), body, file_size);

    free(body);
}

// This is what each worker thread actually runs. It receives the client
// socket fd (passed in via a heap-allocated int, see below), handles that
// one connection fully, then exits -- ending the thread.
void *handle_client(void *arg) {
    int client_fd = *(int *)arg;
    free(arg); // we heap-allocated this in main() just to pass the fd safely into the thread

    char buffer[BUFFER_SIZE];
    memset(buffer, 0, BUFFER_SIZE);
    ssize_t bytes_read = read(client_fd, buffer, BUFFER_SIZE - 1);

    if (bytes_read > 0) {
        char path[1024];
        if (parse_request_path(buffer, path, sizeof(path))) {
            safe_log("[thread %lu] GET %s\n", pthread_self(), path);
            serve_file(client_fd, path);
        } else {
            send_response(client_fd, "400 Bad Request", "text/plain",
                          "Bad Request", strlen("Bad Request"));
        }
    }

    close(client_fd);
    return NULL;
}

int main() {
    int server_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);

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

    if (listen(server_fd, 128) < 0) { // bigger backlog now that we can handle concurrent load
        perror("listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Server (multi-threaded) listening on http://localhost:%d\n", PORT);
    printf("Serving files from ./%s/\n\n", WWW_ROOT);

    while (1) {
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("accept failed");
            continue;
        }

        // Heap-allocate the fd so each thread gets its own stable copy.
        // (If we passed &client_fd directly, the next loop iteration could
        // overwrite it before the new thread reads it -- a classic race bug.)
        int *client_fd_ptr = malloc(sizeof(int));
        *client_fd_ptr = client_fd;

        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, handle_client, client_fd_ptr) != 0) {
            perror("pthread_create failed");
            free(client_fd_ptr);
            close(client_fd);
            continue;
        }

        // Detach the thread: we don't need to pthread_join() it later.
        // It cleans itself up automatically when handle_client() returns.
        pthread_detach(thread_id);
    }

    close(server_fd);
    return 0;
}

/*
 * BENCHMARKING (do this once Phase 3 compiles and runs):
 *
 * Install a load-testing tool if you don't have one:
 *   brew install wrk
 *
 * Run server_phase2 (single-threaded) in one terminal, then in another:
 *   wrk -t4 -c50 -d10s http://localhost:8888/
 *   (4 threads generating load, 50 concurrent connections, 10 seconds)
 *
 * Note the "Requests/sec" number, then stop server_phase2 and repeat the
 * exact same wrk command against server_phase3 (multi-threaded).
 *
 * Compare the two "Requests/sec" numbers -- THAT difference is your real,
 * measured concurrency improvement, and exactly the kind of before/after
 * metric worth putting on a resume.
 */
