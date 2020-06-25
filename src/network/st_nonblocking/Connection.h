#ifndef AFINA_NETWORK_ST_NONBLOCKING_CONNECTION_H
#define AFINA_NETWORK_ST_NONBLOCKING_CONNECTION_H

#include <afina/execute/Command.h>
#include <cstring>
#include <protocol/Parser.h>
#include <spdlog/logger.h>
#include <sys/epoll.h>
#include <vector>
namespace Afina {
namespace Network {
namespace STnonblock {

class Connection {
public:
    Connection(int s, std::shared_ptr<Afina::Storage> &ps, std::shared_ptr<spdlog::logger> &pl)
        : _socket(s), pStorage(ps), _logger(pl) {
        std::memset(&_event, 0, sizeof(struct epoll_event));
        _event.data.ptr = this;
        is_alive = true;
        arg_remains = 0;
        readed_bytes = 0;
        shift = 0;
    }

    inline bool isAlive() const { return is_alive; }

    void Start();

protected:
    void OnError();
    void OnClose();
    void DoRead();
    void DoWrite();

private:
    friend class ServerImpl;

    int _socket;
    struct epoll_event _event;

    bool is_alive;

    std::shared_ptr<spdlog::logger> _logger;
    std::shared_ptr<Afina::Storage> pStorage;

    // Here is connection state
    // - parser: parse state of the stream
    // - command_to_execute: last command parsed out of stream
    // - arg_remains: how many bytes to read from stream to get command argument
    // - argument_for_command: buffer stores argument
    std::size_t arg_remains;
    Protocol::Parser parser;
    std::string argument_for_command;
    std::unique_ptr<Execute::Command> command_to_execute;

    int readed_bytes;
    char client_buffer[4096];

    std::vector<std::string> output_vec;

    int shift;
};

} // namespace STnonblock
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_ST_NONBLOCKING_CONNECTION_H
