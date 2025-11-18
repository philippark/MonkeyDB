// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
// system
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
// C++
#include <vector>
#include <string>

// maximum allowed message size
const size_t K_MAX_MSG = 32 << 20;

// maximum allowed arguments for a command
const size_t K_MAX_ARGS = 200 * 1000;

// Representation for a single client connection
struct Conn {
    int fd { -1 };
    // operation 
    bool want_read { false }; 
    bool want_write { false };
    bool want_close { false };
    // input and output buffers
    std::vector<uint8_t> incoming;   
    std::vector<uint8_t> outgoing; 
};

static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void msg_errno(const char *msg) {
    fprintf(stderr, "[errno:%d] %s\n", errno, msg);
}

static void die(const char *msg) {
    int err { errno };
    fprintf(stderr, "[%d] %s\n", err, msg);   
    abort();
}

// set a file descriptor to non-blocking mode 
static void fd_set_nb(int fd) {
    errno = 0;
    int flags { fcntl(fd, F_GETFL, 0) };
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    static_cast<void>(fcntl(fd, F_SETFL, flags));
    if (errno) {
        die("fcntl error");
    }
}

// append to the back of a buffer
static void buf_append(std::vector<uint8_t> &buf, const uint8_t *data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

// remove from the front of a buffer
static void buf_consume(std::vector<uint8_t> &buf, size_t n) {
    buf.erase(buf.begin(), buf.begin() + n);
}

// reads a unsigned 32 byte int from cur into out
static bool read_u32(const uint8_t *&cur, const uint8_t *end, uint32_t &out) {
    if (cur + 4 > end) {
        return false;
    }

    memcpy(&out, cur, 4);
    cur += 4;
    return true;
}

// reads in a string of length len from cur into out
static bool read_str(const uint8_t *&cur, const uint8_t *end, size_t len, std::string &out) {
    if (cur + len > end) {
        return false;
    }
    
    out.assign(cur, cur + len);
    cur += len;
    return true;
}

// parses a request that contains a list of strings
// protocol: nstr len1 str1 len2 str2 ...
// nstr is the length of the whole list, and each string is length-prefixed
static int32_t parse_req(const uint8_t *data, size_t size, std::vector<std::string> &out) {
    const uint8_t *end { data + size };
    uint32_t nstr { 0 };

    if (!read_u32(data, end, nstr)) {
        return -1;
    }

    if (nstr > K_MAX_ARGS) {
        return -1;
    }

    while (out.size() < nstr) {
        uint32_t len { 0 };

        if (!read_u32(data, end, len)) {
            return -1;
        }

        out.push_back(std::string());

        if (!read_str(data, end, len, out.back())) {
            return -1;
        }
    }

    if (data != end) {
        return -1;
    }

    return 0;
}

// handles one request in the form of a length prefixed protocol
static bool try_one_request(Conn *conn) {
    // need at least 4 bytes for the length
    if (conn->incoming.size() < 4) {
        return false;
    }

    uint32_t len { 0 };
    memcpy(&len, conn->incoming.data(), 4);

    if (len > K_MAX_MSG) {
        msg("too long");
        conn->want_close = true;
        return false;
    }

    if (4 + len > conn->incoming.size()) {
        return false;
    }

    const uint8_t *request { &conn->incoming[4] };

    buf_append(conn->outgoing, reinterpret_cast<const uint8_t *>(&len), 4);
    buf_append(conn->outgoing, request, len);

    buf_consume(conn->incoming, 4 + len);

    return true;
}

// accepts a new client connection on given file descriptor
// sets the fd to non-blocking mode and creates the Conn object
static Conn *handle_accept(int fd) {
    struct sockaddr_in client_addr { {} };
    socklen_t addrlen { sizeof(client_addr) };
    
    int connfd = accept(fd, reinterpret_cast<struct sockaddr *>(&client_addr), &addrlen);
    if (connfd < 0) {
        msg_errno("accept() error");
        return NULL;
    }

    // just for debugging
    uint32_t ip { client_addr.sin_addr.s_addr };
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
        ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
        ntohs(client_addr.sin_port)
    );

    fd_set_nb(connfd);

    Conn *conn { new Conn() };
    conn->fd = connfd;
    conn->want_read = true; 
    return conn;
}

// handles writing responses
static void handle_write(Conn *conn) {
    assert(conn->outgoing.size() > 0);
    ssize_t rv { write(conn->fd, conn->outgoing.data(), conn->outgoing.size()) };
    
    // a partial write
    if (rv < 0 && errno == EAGAIN) {
        return;
    }
    
    if (rv < 0) {
        msg_errno("write() error");
        conn->want_close = true;
        return;
    }

    buf_consume(conn->outgoing, static_cast<size_t>(rv));

    if (conn->outgoing.size() == 0) { 
        conn->want_read = true;
        conn->want_write = false;
    }
}

// handles reading requests
static void handle_read(Conn *conn) {
    uint8_t buf[64 * 1024];
    ssize_t rv { read(conn->fd, buf, sizeof(buf)) };
    
    // partial read
    if (rv < 0 && errno == EAGAIN) {
        return;
    }
    
    if (rv < 0) {
        msg_errno("read() error");
        conn->want_close = true;
        return;
    }

    if (rv == 0) {
        if (conn->incoming.size() == 0) {
            msg("client closed");
        } else {
            msg("unexpected EOF");
        }
        conn->want_close = true;
        return;
    }

    buf_append(conn->incoming, buf, static_cast<size_t>(rv));

    while (try_one_request(conn)) {}

    if (conn->outgoing.size() > 0) {
        conn->want_read = false;
        conn->want_write = true;

        return handle_write(conn);
    }
}

int main() {
    // create listening socket
    int fd { socket(AF_INET, SOCK_STREAM, 0) };
    if (fd < 0) {
        die("socket()");
    }
    int val { 1 };
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // bind
    struct sockaddr_in addr { {} };
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);
    int rv { bind(fd, (const struct sockaddr *)&addr, sizeof(addr)) };
    if (rv) {
        die("bind()");
    }

    // set the listen fd to nonblocking mode
    fd_set_nb(fd);

    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }

    // map of all client connections indexed by their fd
    std::vector<Conn*> fd2conn;

    // event loop
    std::vector<struct pollfd> poll_args;
    while (true) {
        poll_args.clear();

        // put listening socket in first
        struct pollfd pfd { fd, POLLIN, 0 };
        poll_args.push_back(pfd);
        
        for (Conn *conn : fd2conn) {
            if (!conn) continue;

            struct pollfd pfd { conn->fd, POLLERR, 0 };

            if (conn->want_read) {
                pfd.events |= POLLIN;
            }
            if (conn->want_write) {
                pfd.events |= POLLOUT;
            }

            poll_args.push_back(pfd);
        }

        int rv { poll(poll_args.data(), static_cast<nfds_t>(poll_args.size()), -1) };
        if (rv < 0 && errno == EINTR) {
            continue;
        }
        if (rv < 0) {
            die("poll");
        }
        
        // handle listening socket
        // accept, create a connection representation, and store in the map 
        if (poll_args[0].revents) {
            if (Conn *conn {handle_accept(fd)}) {
                if (fd2conn.size() <= static_cast<size_t>(conn->fd)) {
                    fd2conn.resize(conn->fd + 1);
                }
                assert(!fd2conn[conn->fd]);
                fd2conn[conn->fd] = conn;
            }
        }

        // handle operations for connections that are ready
        for (size_t i = 1; i < poll_args.size(); ++i) {
            uint32_t ready { poll_args[i].revents };
            Conn *conn { fd2conn[poll_args[i].fd] };

            if (ready & POLLIN) {
                handle_read(conn);
            }
            if (ready & POLLOUT) {
                handle_write(conn);
            }

            if ((ready & POLLERR) || (conn->want_close)) {
                static_cast<void>(close(conn->fd));
                fd2conn[conn->fd] = NULL;
                delete conn;
            }
        }
    }

    return 0;
}