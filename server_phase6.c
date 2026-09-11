/*
 * Phase 6: In-memory file cache (attacking the real throughput ceiling)
 *
 * What the Phase 5 benchmarks told us: throughput was pinned at ~25.9k
 * req/sec regardless of worker count (8 vs 64) or connection count (8 vs 50).
 * Concurrency was NOT the bottleneck -- something per-request was.
 *
 * The suspect: every single request was doing
 *   fopen() -> stat() -> malloc() -> fread() -> fclose() -> free()
 * That's a disk read (or at least a page-cache syscall round trip) plus a
 * heap allocation and free, ~25,900 times per second.
 *
 * The fix: load every file in www/ into memory ONCE at startup. Requests
 * then just point at an already-resident buffer -- no syscalls, no malloc,
 * no copy. This is what real static-file servers do (nginx does something
 * similar with its open-file cache + sendfile).
 *
 * New concepts introduced:
 * - Reading a directory at startup (opendir/readdir) to preload files
 * - A simple in-memory cache struct with O(n) lookup (n is tiny here)
 * - Zero-allocation request path: response header is built on the stack,
 *   body is a pointer into the cache
 *
 * Build:  gcc -o server_phase6 server_phase6.c -lpthread
 * Run:    ./server_phase6
 * Bench:  wrk -t4 -c50 -d10s http://localhost:8888/
 *
 * If the theory is right, throughput should break past the ~25.9k ceiling.
 * If it does NOT move, that's also a real finding: it means the ceiling is
 * in the network/syscall path (read/write per request, loopback stack), not
 * in file I/O -- and the next thing to attack would be the syscall count
 * per request itself.
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
#include <dirent.h>

#define PORT 8888
#define BUFFER_SIZE 4096
#define WWW_ROOT "www"
#define THREAD_POOL_SIZE 8
#define QUEUE_CAPACITY 256
#define KEEPALIVE_TIMEOUT_SEC 5
#define MAX_REQUESTS_PER_CONN 1000
#define MAX_CACHED_FILES 64
#define MAX_PATH_LEN 512

// ---------- In-memory file cache ----------
// Loaded once at startup, read-only afterward, so no locking is needed:
// many threads can safely read the same immutable buffers concurrently.
typedef struct {
    char path[MAX_PATH_LEN];      // e.g. "/index.html"
    char *data;                   // file contents, held in memory
    size_t size;
    const char *content_type;
} CachedFile;

CachedFile file_cache[MAX_CACHED_FILES];
int cached_file_count = 0;

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

// Walk the www/ directory once at startup and slurp every regular file
// into memory. Returns the number of files cached.
int load_file_cache(const char *root) {
    DIR *dir = opendir(root);
    if (!dir) {
        perror("opendir failed");
        return 0;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL && cached_file_count < MAX_CACHED_FILES) {
        // Skip "." and ".." and anything hidden
        if (entry->d_name[0] == '.') continue;

        char full_path[MAX_PATH_LEN * 2];
        snprintf(full_path, sizeof(full_path), "%s/%s", root, entry->d_name);

        struct stat st;
        if (stat(full_path, &st) != 0) continue;
        if (!S_ISREG(st.st_mode)) continue; // skip subdirectories for now

        FILE *fp = fopen(full_path, "rb");
        if (!fp) continue;

        char *data = malloc(st.st_size);
        if (!data) {
            fclose(fp);
            continue;
        }

        size_t read_bytes = fread(data, 1, st.st_size, fp);
        fclose(fp);

        if (read_bytes != (size_t)st.st_size) {
            free(data);
            continue;
        }

        CachedFile *cf = &file_cache[cached_file_count];
        snprintf(cf->path, sizeof(cf->path), "/%s", entry->d_name); // URL-style path
        cf->data = data;
        cf->size = st.st_size;
        cf->content_type = get_content_type(entry->d_name);

        printf("  cached %s (%zu bytes, %s)\n", cf->path, cf->size, cf->content_type);
        cached_file_count++;
    }

    closedir(dir);
    return cached_file_count;
}

// Linear scan is fine here: we have a handful of files, and a linear scan
// over a few cache-resident structs is faster than hashing for n this small.
CachedFile *cache_lookup(const char *path) {
    for (int i = 0; i < cached_file_count; i++) {
        if (strcmp(file_cache[i].path, path) == 0) {
            return &file_cache[i];
        }
    }
    return NULL;
}

// ---------- Thread-safe connection queue (unchanged from Phase 4/5) ----------
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

// ---------- HTTP handling ----------
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

int client_wants_close(const char *request) {
    return strcasestr(request, "Connection: close") != NULL;
}

// Builds header on the STACK and writes header+body in a single writev()-style
// combined buffer. No malloc in the request path at all now.
void send_cached_response(int client_fd, const char *status, const char *content_type,
                           const char *body, size_t body_len, int keep_alive) {
    // Stack buffer sized for header + typical small file. If the body is
    // bigger than what fits, we fall back to two writes (still correct,
    // just slightly less optimal for very large files).
    char out[8192];

    int header_len = snprintf(out, sizeof(out),
        "HTTP/1.1 %s\r\n"
        "Content-Type: %s\r\n"
        "Content-Length: %zu\r\n"
        "Connection: %s\r\n"
        "\r\n",
        status, content_type, body_len, keep_alive ? "keep-alive" : "close");

    if ((size_t)header_len + body_len <= sizeof(out)) {
        memcpy(out + header_len, body, body_len);
        write(client_fd, out, header_len + body_len);
    } else {
        write(client_fd, out, header_len);
        write(client_fd, body, body_len);
    }
}

void send_404(int client_fd, int keep_alive) {
    static const char body[] = "<html><body><h1>404 Not Found</h1></body></html>";
    send_cached_response(client_fd, "404 Not Found", "text/html",
                         body, sizeof(body) - 1, keep_alive);
}

void handle_connection(int client_fd) {
    char buffer[BUFFER_SIZE];

    int nodelay = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    struct timeval tv;
    tv.tv_sec = KEEPALIVE_TIMEOUT_SEC;
    tv.tv_usec = 0;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    for (int req_count = 0; req_count < MAX_REQUESTS_PER_CONN; req_count++) {
        ssize_t bytes_read = read(client_fd, buffer, BUFFER_SIZE - 1);
        if (bytes_read <= 0) break;
        buffer[bytes_read] = '\0';

        int keep_alive = !client_wants_close(buffer);

        char path[1024];
        if (parse_request_path(buffer, path, sizeof(path))) {
            // Map "/" to "/index.html"
            const char *lookup_path = (strcmp(path, "/") == 0) ? "/index.html" : path;

            // THE KEY CHANGE: no fopen/fread/malloc -- just a pointer lookup
            // into memory that was populated once at startup.
            CachedFile *cf = cache_lookup(lookup_path);
            if (cf) {
                send_cached_response(client_fd, "200 OK", cf->content_type,
                                     cf->data, cf->size, keep_alive);
            } else {
                send_404(client_fd, keep_alive);
            }
        } else {
            static const char bad[] = "Bad Request";
            send_cached_response(client_fd, "400 Bad Request", "text/plain",
                                 bad, sizeof(bad) - 1, keep_alive);
        }

        if (!keep_alive) break;
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

    printf("Loading files into memory cache...\n");
    int n = load_file_cache(WWW_ROOT);
    if (n == 0) {
        fprintf(stderr, "WARNING: no files cached from ./%s/ -- all requests will 404\n", WWW_ROOT);
    }
    printf("Cached %d file(s)\n\n", n);

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

    printf("Server (thread pool + keep-alive + file cache, %d workers) listening on http://localhost:%d\n",
           THREAD_POOL_SIZE, PORT);
    printf("Serving %d cached file(s) from ./%s/\n\n", cached_file_count, WWW_ROOT);

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
