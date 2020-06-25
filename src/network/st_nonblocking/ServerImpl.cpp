#include "ServerImpl.h"

#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <spdlog/logger.h>

#include <afina/Storage.h>
#include <afina/logging/Service.h>

#include "Connection.h"
#include "Utils.h"

namespace Afina {
namespace Network {
namespace STnonblock {

// See Server.h
ServerImpl::ServerImpl(std::shared_ptr<Afina::Storage> ps, std::shared_ptr<Logging::Service> pl) : Server(ps, pl) {}

// See Server.h
ServerImpl::~ServerImpl() {}

// See Server.h
void ServerImpl::Start(uint16_t port, uint32_t n_acceptors, uint32_t n_workers) {
    _logger = pLogging->select("network");
    _logger->info("Start st_nonblocking network service");

    sigset_t sig_mask;
    sigemptyset(&sig_mask);
    sigaddset(&sig_mask, SIGPIPE);
    if (pthread_sigmask(SIG_BLOCK, &sig_mask, NULL) != 0) {
        throw std::runtime_error("Unable to mask SIGPIPE");
    }

    // Create server socket
    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;         // IPv4
    server_addr.sin_port = htons(port);       // TCP port number
    server_addr.sin_addr.s_addr = INADDR_ANY; // Bind to any address

    _server_socket = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_server_socket == -1) {
        throw std::runtime_error("Failed to open socket: " + std::string(strerror(errno)));
    }

    int opts = 1;
    if (setsockopt(_server_socket, SOL_SOCKET, (SO_KEEPALIVE), &opts, sizeof(opts)) == -1) {
        close(_server_socket);
        throw std::runtime_error("Socket setsockopt() failed: " + std::string(strerror(errno)));
    }

    if (bind(_server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        close(_server_socket);
        throw std::runtime_error("Socket bind() failed: " + std::string(strerror(errno)));
    }

    make_socket_non_blocking(_server_socket);
    if (listen(_server_socket, 5) == -1) {
        close(_server_socket);
        throw std::runtime_error("Socket listen() failed: " + std::string(strerror(errno)));
    }

    _event_fd = eventfd(0, EFD_NONBLOCK);
    if (_event_fd == -1) {
        throw std::runtime_error("Failed to create epoll file descriptor: " + std::string(strerror(errno)));
    }

    _work_thread = std::thread(&ServerImpl::OnRun, this);
}

// See Server.h
void ServerImpl::Stop() {
    _logger->warn("Stop network service");

    // Wakeup threads that are sleep on epoll_wait
    if (eventfd_write(_event_fd, 1)) {
        throw std::runtime_error("Failed to wakeup workers");
    }

    shutdown(_server_socket, SHUT_RDWR);
}

// See Server.h
void ServerImpl::Join() {
    // Wait for work to be complete
    _work_thread.join();
    close(_server_socket);
}

// See ServerImpl.h
void ServerImpl::OnRun() {
    _logger->info("Start acceptor");
    int epoll_descr = epoll_create1(0);
    if (epoll_descr == -1) {
        throw std::runtime_error("Failed to create epoll file descriptor: " + std::string(strerror(errno)));
    }

    struct epoll_event event;
    /*
    EPOLLIN
    Ассоциированный файл доступен для операций read.
    */
    event.events = EPOLLIN;
    event.data.fd = _server_socket;
    // int epoll_ctl(int epfd, int op, int fd, struct epoll_event *event)
    /*
    Управляет описателем epoll - epfd - через запрос выполнения операции op на цели, описателе файла fd. Событие event
    описывает объект, привязанный к описателю файла fd.
    */
    if (epoll_ctl(epoll_descr, EPOLL_CTL_ADD, _server_socket, &event)) {
        throw std::runtime_error("Failed to add file descriptor to epoll");
    }

    struct epoll_event event2;
    event2.events = EPOLLIN;
    event2.data.fd = _event_fd;
    if (epoll_ctl(epoll_descr, EPOLL_CTL_ADD, _event_fd, &event2)) {
        throw std::runtime_error("Failed to add file descriptor to epoll");
    }

    bool run = true;
    std::array<struct epoll_event, 64> mod_list;
    while (run) {

        /*
               int epoll_wait(int epfd, struct epoll_event *events,
                      int maxevents, int timeout);

       The epoll_wait() system call waits for events on the epoll
       instance referred to by the file descriptor epfd.  The buffer pointed
       to by events is used to return information from the ready list about
       file descriptors in the interest list that have some events
       available.  Up to maxevents are returned by epoll_wait().  The
       maxevents argument must be greater than zero.

       The timeout argument specifies the number of milliseconds that
       epoll_wait() will block.  Time is measured against the
       CLOCK_MONOTONIC clock.

       A call to epoll_wait() will block until either:

       · a file descriptor delivers an event;

       · the call is interrupted by a signal handler; or

       · the timeout expires.

       Note that the timeout interval will be rounded up to the system clock
       granularity, and kernel scheduling delays mean that the blocking
       interval may overrun by a small amount.  Specifying a timeout of -1
       causes epoll_wait() to block indefinitely, while specifying a timeout
       equal to zero cause epoll_wait() to return immediately, even if no
       events are available.

              When successful, epoll_wait() returns the number of file descriptors
       ready for the requested I/O, or zero if no file descriptor became
       ready during the requested timeout milliseconds.  When an error
       occurs, epoll_wait() returns -1 and errno is set appropriately.
        */
        int nmod = epoll_wait(epoll_descr, &mod_list[0], mod_list.size(), -1);
        _logger->debug("Acceptor wokeup: {} events", nmod);

        for (int i = 0; i < nmod; i++) {
            struct epoll_event &current_event = mod_list[i];
            if (current_event.data.fd == _event_fd) {
                _logger->debug("Break acceptor due to stop signal");
                run = false;
                continue;
            } else if (current_event.data.fd == _server_socket) {
                OnNewConnection(epoll_descr);
                continue;
            }

            // That is some connection!
            Connection *pc = static_cast<Connection *>(current_event.data.ptr);

            auto old_mask = pc->_event.events;
            if ((current_event.events & EPOLLERR) || (current_event.events & EPOLLHUP)) {
                pc->OnError();
            } else if (current_event.events & EPOLLRDHUP) {
                pc->OnClose();
            } else {
                // Depends on what connection wants...
                if (current_event.events & EPOLLIN) {
                    pc->DoRead();
                }
                if (current_event.events & EPOLLOUT) {
                    pc->DoWrite();
                }
            }

            // Is it alive?
            if (!pc->isAlive()) {
                if (epoll_ctl(epoll_descr, EPOLL_CTL_DEL, pc->_socket, &pc->_event)) {
                    _logger->error("Failed to delete connection from epoll");
                }

                close(pc->_socket);
                pc->OnClose();

                delete pc;
            } else if (pc->_event.events != old_mask) {
                if (epoll_ctl(epoll_descr, EPOLL_CTL_MOD, pc->_socket, &pc->_event)) {
                    _logger->error("Failed to change connection event mask");

                    close(pc->_socket);
                    cns.erase(pc);
                    pc->OnClose();

                    delete pc;
                }
            }
        }
    }
    for (auto one_cns : cns) {
        close(one_cns->_socket);
        delete one_cns;
    }
    cns.clear();
    _logger->warn("Acceptor stopped");
}

void ServerImpl::OnNewConnection(int epoll_descr) {
    for (;;) {
        struct sockaddr in_addr;
        socklen_t in_len;

        // No need to make these sockets non blocking since accept4() takes care of it.
        in_len = sizeof in_addr;

        /*
        int accept4(int sockfd, struct sockaddr *addr, socklen_t *addrlen, int flags);
        The accept() system call is used with connection-based socket types (SOCK_STREAM, SOCK_SEQPACKET). It extracts
the first connection request on the queue of pending connections for the listening socket, sockfd, creates a new
connected socket, and returns a new file descriptor referring to that socket. The newly created socket is not in the
listening state. The original socket sockfd is unaffected by this call.

If no pending connections are present on the queue, and the socket is not marked as nonblocking, accept() blocks the
caller until a connection is present. If the socket is marked nonblocking and no pending connections are present on the
queue, accept() fails with the error EAGAIN or EWOULDBLOCK.
        */
        int infd = accept4(_server_socket, &in_addr, &in_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (infd == -1) {
            if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                break; // We have processed all incoming connections.
            } else {
                _logger->error("Failed to accept socket");
                break;
            }
        }

        // Print host and service info.
        char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
        int retval =
            getnameinfo(&in_addr, in_len, hbuf, sizeof hbuf, sbuf, sizeof sbuf, NI_NUMERICHOST | NI_NUMERICSERV);
        if (retval == 0) {
            _logger->info("Accepted connection on descriptor {} (host={}, port={})\n", infd, hbuf, sbuf);
        }

        // Register the new FD to be monitored by epoll.
        Connection *pc = new (std::nothrow) Connection(infd, pStorage, _logger);
        if (pc == nullptr) {
            throw std::runtime_error("Failed to allocate connection");
        }
        cns.insert(pc);
        // Register connection in worker's epoll
        pc->Start();
        if (pc->isAlive()) {
            if (epoll_ctl(epoll_descr, EPOLL_CTL_ADD, pc->_socket, &pc->_event)) {
                pc->OnError();
                cns.erase(pc);
                delete pc;
            }
        }
    }
}

} // namespace STnonblock
} // namespace Network
} // namespace Afina
