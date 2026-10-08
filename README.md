# Multi-Threaded HTTP Server

A static-file HTTP server written in **C**, built in five incremental versions to explore socket programming, concurrency, and performance bottlenecks. The final version combines a fixed worker pool, persistent connections, and an immutable in-memory file cache.

## Engineering highlights

- **Bounded producer–consumer queue:** the accept loop hands sockets to workers through a 256-slot ring buffer protected by a mutex and condition variables.
- **Connection reuse:** keep-alive avoids opening a new TCP connection for every request, with a 5-second receive timeout and a 1,000-request cap per connection.
- **Cached file serving:** regular files in `www/` are loaded before workers start, removing per-request file reads and heap allocations from the final version.
- **Small-response batching:** headers and small bodies share one stack buffer and one write; `TCP_NODELAY` is enabled for accepted connections.

## Implementation progression

| Source | Design | Reason for the change |
| --- | --- | --- |
| [Phase 2](server_phase2.c) | Sequential static-file server | Establish request parsing, MIME types, and 404 responses |
| [Phase 3](server_phase3.c) | One thread per connection | Allow concurrent clients |
| [Phase 4](server_phase4.c) | Fixed 64-worker pool | Avoid repeatedly creating OS threads |
| [Phase 5](server_phase5.c) | Pool plus keep-alive | Reuse connections and batch response writes |
| [Phase 6](server_phase6.c) | 8-worker pool plus file cache | Remove repeated file I/O and allocation |

Each source file is a standalone executable. The repository starts at Phase 2.

## Build and run

Use Linux with GCC, POSIX sockets, and pthreads. The keep-alive versions use GNU `strcasestr`, so enable GNU extensions when compiling.

```bash
git clone https://github.com/kylehtet/Multi-Threaded-HTTP-Server.git
cd Multi-Threaded-HTTP-Server
gcc -D_GNU_SOURCE -O2 -Wall -Wextra -pthread server_phase6.c -o server_phase6
./server_phase6
```

Run from the repository root so the server can find `www/`. It listens on port **8888**. Stop it with Ctrl+C.

In another terminal:

```bash
curl -i http://localhost:8888/
curl -i http://localhost:8888/about.html
curl -i http://localhost:8888/doesnotexist.html
```

These demonstrate the index page, another static file, and a 404 response. Only GET is supported; other methods receive 400.

To compare an earlier version, compile its source and run it instead. Run one version at a time because all use the same port.

## Architecture and tradeoffs

The main thread accepts sockets and enqueues them. Workers sleep when the queue is empty; the accept loop waits when the queue is full. A worker owns a connection until it closes or reaches its timeout/request cap.

Phase 6 loads up to **64 non-hidden regular files** from the top level of `www/`. The cache remains read-only after startup, allowing workers to share file contents without a cache lock. Lookup is a linear scan, suitable for this small file set. File changes require a restart, and nested directories are not loaded.

A worker per active connection is easy to reason about, but idle keep-alive clients can occupy the entire pool. An event-driven design would be a possible next step.

## Benchmarking

The Phase 6 source records an earlier Phase 5 throughput of approximately **25.9K requests/second**. Raw benchmark logs and machine specifications are not included, so treat this as a recorded development observation rather than a reproducible cross-machine performance claim.

With `wrk` installed, run this in a separate terminal while the server is running:

```bash
wrk -t4 -c50 -d10s http://localhost:8888/
```

Compare versions using the same compiler flags, machine, file, concurrency, and duration. Record throughput, average latency, and socket errors. The final version also changes worker count, so isolate that setting if measuring the cache's contribution.

## Scope and limitations

This is a learning and benchmarking project with a deliberately small HTTP implementation. Requests are parsed from a single read; fragmented headers and HTTP pipelining are not fully handled, and writes do not retry partial sends. Earlier disk-serving versions concatenate request paths without traversal protection. TLS, graceful shutdown, and complete HTTP compliance are outside the current implementation.
