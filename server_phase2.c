/*
 * Phase 2: Parse the real request path and serve actual files
 *
 * New in this phase:
 * - Parse the HTTP request line to extract the requested path (e.g. "/about.html")
 * - Map "/" to "/index.html"
 * - Open and read the corresponding file from the www/ directory
 * - Detect Content-Type based on file extension
 * - Return a proper 404 if the file doesn't exist
 *
 * Build:  gcc -o server_phase2 server_phase2.c
 * Run:    ./server_phase2
 * Test:   curl http://localhost:8888/
 *         curl http://localhost:8888/about.html
 *         curl http://localhost:8888/doesnotexist.html
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>

#define PORT 8888
#define BUFFER_SIZE 4096
#define WWW_ROOT "www"

// Given a file path like "index.html", return the right Content-Type header value.
// Falls back to "application/octet-stream" for unknown extensions.
const char *get_content_type(const char *path) {
    const char *ext = strrchr(path, '.'); // find the last '.' in the filename
    if (!ext) return "application/octet-stream";

    if (strcmp(ext, ".html") == 0) return "text/html";
    if (strcmp(ext, ".css") == 0)  return "text/css";
    if (strcmp(ext, ".js") == 0)   return "application/javascript";
    if (strcmp(ext, ".png") == 0)  return "image/png";
    if (strcmp(ext, ".jpg") == 0)  return "image/jpeg";
    if (strcmp(ext, ".txt") == 0)  return "text/plain";

    return "application/octet-stream";
}

// Parses "GET /about.html HTTP/1.1\r\n..." and extracts just "/about.html" into out_path.
// Returns 1 on success, 0 if the request line couldn't be parsed.
int parse_request_path(const char *request, char *out_path, size_t out_size) {
    char method[16];
    char path[1024];

    // sscanf reads the first line: "%15s %1023s" grabs method and path, skips the HTTP version
    if (sscanf(request, "%15s %1023s", method, path) != 2) {
        return 0;
    }

    // We only support GET for now
    if (strcmp(method, "GET") != 0) {
        return 0;
    }

    strncpy(out_path, path, out_size - 1);
    out_path[out_size - 1] = '\0';
    return 1;
}

// Sends a full HTTP response with the given status line, content type, and body.
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

// Reads the requested file from disk and sends it back, or sends a 404 if it's missing.
void serve_file(int client_fd, const char *requested_path) {
    char full_path[1200];

    // Map "/" to "/index.html"
    if (strcmp(requested_path, "/") == 0) {
        requested_path = "/index.html";
    }

    // Build the real filesystem path: www + requested_path
    snprintf(full_path, sizeof(full_path), "%s%s", WWW_ROOT, requested_path);

    // SECURITY NOTE: this version does not defend against "../" path traversal.
    // A real server must sanitize the path before trusting it. We'll flag this
    // as a known limitation for now and can fix it in a later pass.

    FILE *fp = fopen(full_path, "rb");
    if (!fp) {
        printf("File not found: %s\n", full_path);
        send_404(client_fd);
        return;
    }

    // Get file size
    struct stat st;
    stat(full_path, &st);
    long file_size = st.st_size;

    // Read the whole file into memory (fine for small files; a later phase
    // could stream large files in chunks instead)
    char *body = malloc(file_size);
    fread(body, 1, file_size, fp);
    fclose(fp);

    printf("Serving %s (%ld bytes)\n", full_path, file_size);
    send_response(client_fd, "200 OK", get_content_type(full_path), body, file_size);

    free(body);
}

int main() {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);
    char buffer[BUFFER_SIZE];

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

    if (listen(server_fd, 10) < 0) {
        perror("listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Server listening on http://localhost:%d\n", PORT);
    printf("Serving files from ./%s/\n\n", WWW_ROOT);

    while (1) {
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("accept failed");
            continue;
        }

        memset(buffer, 0, BUFFER_SIZE);
        ssize_t bytes_read = read(client_fd, buffer, BUFFER_SIZE - 1);

        if (bytes_read > 0) {
            char path[1024];
            if (parse_request_path(buffer, path, sizeof(path))) {
                printf("GET %s\n", path);
                serve_file(client_fd, path);
            } else {
                // Couldn't parse a valid GET request
                send_response(client_fd, "400 Bad Request", "text/plain",
                              "Bad Request", strlen("Bad Request"));
            }
        }

        close(client_fd);
    }

    close(server_fd);
    return 0;
}
