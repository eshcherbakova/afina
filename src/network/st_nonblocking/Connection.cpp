#include "Connection.h"

#include <errno.h>
#include <iostream>
#include <sys/socket.h>
#include <sys/uio.h>
namespace Afina {
namespace Network {
namespace STnonblock {

// See Connection.h
void Connection::Start() {

    /*
    The struct epoll_event is defined as:

               typedef union epoll_data {
                   void    *ptr;
                   int      fd;
                   uint32_t u32;
                   uint64_t u64;
               } epoll_data_t;

               struct epoll_event {
                   uint32_t     events;     Epoll events
                   epoll_data_t data;       User data variable
               };
    */

    _logger->debug("Start connection on {} socket", _socket);
    _event.data.fd = _socket;
    _event.data.ptr = this;
    _event.events = EPOLLIN | EPOLLHUP | EPOLLERR;
}

// See Connection.h
void Connection::OnError() {
    _logger->debug("Error on {} socket", _socket);
    is_alive = false;
}

// See Connection.h
void Connection::OnClose() {
    _logger->debug("Connection on {} socket closed", _socket);
    is_alive = false;
}

// See Connection.h
void Connection::DoRead() {
    try {
        int cur_readed_bytes = -1;
        while ((cur_readed_bytes = read(_socket, client_buffer, sizeof(client_buffer))) > 0) {
            _logger->debug("Got {} bytes from socket", readed_bytes);
            readed_bytes += cur_readed_bytes;
            // Single block of data readed from the socket could trigger inside actions a multiple times,
            // for example:
            // - read#0: [<command1 start>]
            // - read#1: [<command1 end> <argument> <command2> <argument for command 2> <command3> ... ]
            while (readed_bytes > 0) {
                _logger->debug("Process {} bytes", readed_bytes);
                // There is no command yet
                if (!command_to_execute) {
                    std::size_t parsed = 0;
                    if (parser.Parse(client_buffer, readed_bytes, parsed)) {
                        // There is no command to be launched, continue to parse input stream
                        // Here we are, current chunk finished some command, process it
                        _logger->debug("Found new command: {} in {} bytes", parser.Name(), parsed);
                        command_to_execute = parser.Build(arg_remains);
                        if (arg_remains > 0) {
                            arg_remains += 2;
                        }
                    }

                    // Parsed might fail to consume any bytes from input stream. In real life that could happen,
                    // for example, because we are working with UTF-16 chars and only 1 byte left in stream
                    if (parsed == 0) {
                        break;
                    } else {
                        std::memmove(client_buffer, client_buffer + parsed, readed_bytes - parsed);
                        readed_bytes -= parsed;
                    }
                }

                // There is command, but we still wait for argument to arrive...
                if (command_to_execute && arg_remains > 0) {
                    _logger->debug("Fill argument: {} bytes of {}", readed_bytes, arg_remains);
                    // There is some parsed command, and now we are reading argument
                    std::size_t to_read = std::min(arg_remains, std::size_t(readed_bytes));
                    argument_for_command.append(client_buffer, to_read);

                    std::memmove(client_buffer, client_buffer + to_read, readed_bytes - to_read);
                    arg_remains -= to_read;
                    readed_bytes -= to_read;
                }

                // There is command & argument - RUN!
                if (command_to_execute && arg_remains == 0) {
                    _logger->debug("Start command execution");

                    std::string result;
                    if (argument_for_command.size()) {
                        argument_for_command.resize(argument_for_command.size() - 2);
                    }
                    command_to_execute->Execute(*pStorage, argument_for_command, result);

                    // Send response
                    result += "\r\n";

                    output_vec.push_back(result);
                    if (!(_event.events & EPOLLOUT)) {
                        _event.events |= EPOLLOUT;
                    }
                    if (send(_socket, result.data(), result.size(), 0) <= 0) {
                        throw std::runtime_error("Failed to send response");
                    }

                    // Prepare for the next command
                    command_to_execute.reset();
                    argument_for_command.resize(0);
                    parser.Reset();
                }
            } // while (readed_bytes)
        }

        if (readed_bytes == 0) {
            _logger->debug("Connection closed");
        } else {
            throw std::runtime_error(std::string(strerror(errno)));
        }
    } catch (std::runtime_error &ex) {
        _logger->error("Failed to process connection on descriptor {}: {}", _socket, ex.what());
        OnError();
    }
}

// See Connection.h
void Connection::DoWrite() {

    _logger->debug("DoWrite on {}", _socket);

    if (output_vec.empty()) {
        return;
    }

    /* struct iovec {
                void  *iov_base;    // Starting address
                size_t iov_len;     // Number of bytes to transfer
            };
    */

    struct iovec iov[output_vec.size()];
    iov[0].iov_len = output_vec[0].size() - shift;
    iov[0].iov_base = &(output_vec[0][0]) + shift;

    for (int i = 1; i < output_vec.size(); i++) {
        iov[i].iov_len = output_vec[i].size();
        iov[i].iov_base = &(output_vec[i][0]);
    }

    int written_bytes = writev(_socket, iov, output_vec.size());

    if (written_bytes <= 0) {
        // EAGAIN          Resource temporarily unavailable
        // EINTR           Interrupted function call
        if (errno == EAGAIN || errno == EINTR) {
            return;
        } else {
            OnError();
        }
    }

    int i = 0;
    for (i = 0; i < output_vec.size(); i++) {
        if (written_bytes - iov[i].iov_len >= 0) {
            written_bytes -= iov[i].iov_len;
        } else {
            break;
        }
    }

    shift = written_bytes;
    output_vec.erase(output_vec.begin(), output_vec.begin() + i);

    if (output_vec.empty()) {
        _event.events = EPOLLIN | EPOLLRDHUP | EPOLLERR;
    }
}

} // namespace STnonblock
} // namespace Network
} // namespace Afina
