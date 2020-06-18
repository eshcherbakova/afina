#include "ServerImpl.h"

#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <spdlog/logger.h>

#include <afina/Storage.h>
#include <afina/execute/Command.h>
#include <afina/logging/Service.h>

#include "protocol/Parser.h"

namespace Afina {
namespace Network {
namespace MTblocking {

// See Server.h
ServerImpl::ServerImpl(std::shared_ptr<Afina::Storage> ps, std::shared_ptr<Logging::Service> pl) : Server(ps, pl) {}

// See Server.h
ServerImpl::~ServerImpl() {}

// See Server.h
void ServerImpl::Start(uint16_t port, uint32_t n_accept, uint32_t n_workers) {

    _logger = pLogging->select("network");
    _logger->info("Start mt_blocking network service");

    sigset_t sig_mask;
    sigemptyset(&sig_mask);
    sigaddset(&sig_mask, SIGPIPE);
    //SIG_BLOCK — добавить сигналы к сигнальной маске процесса (заблокировать доставку)
    if (pthread_sigmask(SIG_BLOCK, &sig_mask, nullptr) != 0) {
        throw std::runtime_error("Unable to mask SIGPIPE");
    }
    
    /*Структура sockaddr_in описывает сокет для работы с протоколами IP. Значение поля sin_family всегда равно AF_INET. Поле sin_port содержит номер порта который намерен занять процесс. Если значение этого поля равно нулю, то операционная система сама выделит свободный номер порта для сокета. Поле sin_addr содержит IP адрес к которому будет привязан сокет.*/
    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;         // IPv4
    server_addr.sin_port = htons(port);       // TCP port number
    server_addr.sin_addr.s_addr = INADDR_ANY; // Bind to any address

   // Server socket to accept connections on
   /*
   Создаёт конечную точку соединения и возвращает файловый дескриптор. Принимает три аргумента:
   domain, указывающий семейство протоколов создаваемого сокета. AF_INET - для сетевого протокола IPv4.
   type. SOCK_STREAM (надёжная потокоориентированная служба (сервис) или потоковый сокет).
   protocol. Протоколы обозначаются символьными константами с префиксом IPPROTO_* (например IPPROTO_TCP или IPPROTO_UDP). Допускается значение    protocol=0 (протокол не указан), в этом случае используется значение по умолчанию для данного вида соединений.
   Функция возвращает −1 в случае ошибки. Иначе она возвращает целое число, представляющее присвоенный дескриптор.*/
    _server_socket = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_server_socket == -1) {
        throw std::runtime_error("Failed to open socket");
    }

    int opts = 1;
    /*
    Для управления параметрами, связанными с сокетом, используют функцию
    int setsockopt(int socket, int level, int option_name,
               const void *option_value, socklen_t option_len).
      Функция setsockopt устанавливает параметр, заданный аргументом option_name на уровне протокола, определенного аргументом level, в значение, на которое указывает параметр option_value. Для присвоения параметра на уровне библиотеки сокетов аргументу level присваивается значение SOL_SOCKET. 
     SO_REUSEADDR
    Разрешает повторное использование локальных адресов (если данная возможность поддерживается используемым протоколом). Параметр имеет логическое значение.
    Параметры, имеющие логическое значение, являются целыми. Значение 0 обозначает, что соответствующий параметр будет отключен, значение 1 обозначает, что параметр будет включен. В случае успешного завершения функция фозвращает ноль, если возникли ошибки, то результат равен -1.           
    */
    if (setsockopt(_server_socket, SOL_SOCKET, SO_REUSEADDR, &opts, sizeof(opts)) == -1) {
        close(_server_socket);
        throw std::runtime_error("Socket setsockopt() failed");
    }
    // Связать сокет с IP-адресом и портом
    /*
    Связывает сокет с конкретным адресом. Когда сокет создается при помощи socket(), он ассоциируется с некоторым семейством адресов, но не с конкретным адресом. До того как сокет сможет принять входящие соединения, он должен быть связан с адресом. bind() принимает три аргумента:

    sockfd — дескриптор, представляющий сокет при привязке
    serv_addr — указатель на структуру sockaddr, представляющую адрес, к которому привязываем.
    addrlen — поле socklen_t, представляющее длину структуры sockaddr.

     Возвращает 0 при успехе и −1 при возникновении ошибки.
    */
    if (bind(_server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        close(_server_socket);
        throw std::runtime_error("Socket bind() failed");
    }
    //Объявить о желании принимать соединения. Слушает порт и ждет, когда будет установлено соединение.
    /*
    Подготавливает привязываемый сокет к принятию входящих соединений. Данная функция применима только к типам сокетов SOCK_STREAM и SOCK_SEQPACKET. Принимает два аргумента:

    sockfd — корректный дескриптор сокета.
    backlog — целое число, означающее число установленных соединений, которые могут быть обработаны в любой момент времени. Операционная система обычно ставит его равным максимальному значению.
После принятия соединения оно выводится из очереди. В случае успеха возвращается 0, в случае возникновения ошибки возвращается −1.
    */
    if (listen(_server_socket, 5) == -1) {
        close(_server_socket);
        throw std::runtime_error("Socket listen() failed");
    }

    running.store(true);
    _thread = std::thread(&ServerImpl::OnRun, this);
}

// See Server.h
    /**
     * Signal all worker threads that server is going to shutdown. After method returns
     * no more connections should be accept, existing connections should stop receive commands,
     * but must wait until currently run commands executed.
     *
     * After existing connections drain each should be closed and once worker has no more connection
     * its thread should be exit
     */
void ServerImpl::Stop() {
    running.store(false);
    //int shutdown(int sockfd, int how);
    /*
    If how is SHUT_RD, further receptions will be disallowed.  If how is SHUT_WR,
    further transmissions will be disallowed.  If how is SHUT_RDWR, further
    receptions and transmissions will be disallowed.
    */
    {
    std::unique_lock<std::mutex> lock(set_blocked);
    for (auto descriptor : worker_descriptors) {
        shutdown(descriptor, SHUT_RD);
    }
    }
    shutdown(_server_socket, SHUT_RDWR);
}

// See Server.h
    /**
     * Blocks calling thread until all workers will be stopped and all resources allocated for the network
     * will be released
     */
void ServerImpl::Join() {

    {
        std::unique_lock<std::mutex> lock(set_blocked);
        //wait causes the current thread to block until the condition variable is notified
        while (running.load() || cur_workers != 0) server_stop.wait(lock);
    }
    
    assert(_thread.joinable());
    _thread.join();
    close(_server_socket);
}

// See Server.h
void ServerImpl::OnRun() {
    cur_workers = 0;
    while (running.load()) {
        _logger->debug("waiting for connection...");

        // The call to accept() blocks until the incoming connection arrives
        int client_socket;
        struct sockaddr client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        if ((client_socket = accept(_server_socket, (struct sockaddr *)&client_addr, &client_addr_len)) == -1) {
            continue;
        }
        // Got new connection
        if (_logger->should_log(spdlog::level::debug)) {
            std::string host = "unknown", port = "-1";

            char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
            if (getnameinfo(&client_addr, client_addr_len, hbuf, sizeof(hbuf), sbuf, sizeof(sbuf),
                            NI_NUMERICHOST | NI_NUMERICSERV) == 0) {
                host = hbuf;
                port = sbuf;
            }
            _logger->debug("Accepted connection on descriptor {} (host={}, port={})\n", client_socket, host, port);
        }

        // Configure read timeout
        {
            struct timeval tv;
            tv.tv_sec = 5; // TODO: make it configurable
            tv.tv_usec = 0;
            setsockopt(client_socket, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof tv);
        }

        // Start new thread and process data from/to connection
        if (cur_workers < max_workers && running.load()) {
           
           cur_workers++;
           {
              std::lock_guard<std::mutex> lg(set_blocked);
              std::thread(&ServerImpl::Worker, this, client_socket).detach();
              worker_descriptors.emplace(client_socket);
           }
        
        }
    }

    // Cleanup on exit...
    _logger->warn("Network stopped");
}


void ServerImpl::Worker(int client_socket) {
    // Here is connection state
    // - parser: parse state of the stream
    // - command_to_execute: last command parsed out of stream
    // - arg_remains: how many bytes to read from stream to get command argument
    // - argument_for_command: buffer stores argument
    std::size_t arg_remains = 0;
    Protocol::Parser parser;
    std::string argument_for_command;
    std::unique_ptr<Execute::Command> command_to_execute;
    _logger->debug("Here");
       // Process new connection:
        // - read commands until socket alive
        // - execute each command
        // - send response
        try {
            int readed_bytes = -1;
            char client_buffer[4096] = "";
            while ((readed_bytes = read(client_socket, client_buffer, sizeof(client_buffer))) > 0) {
                _logger->debug("Got {} bytes from socket", readed_bytes);

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

                    // Thre is command & argument - RUN!
                    if (command_to_execute && arg_remains == 0) {
                        _logger->debug("Start command execution");

                        std::string result;
                        if (argument_for_command.size()) {
                            argument_for_command.resize(argument_for_command.size() - 2);
                        }
                        command_to_execute->Execute(*pStorage, argument_for_command, result);

                        // Send response
                        result += "\r\n";
                        if (send(client_socket, result.data(), result.size(), 0) <= 0) {
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
            _logger->error("Failed to process connection on descriptor {}: {}", client_socket, ex.what());
        }

        // We are done with this connection
    {
        std::lock_guard<std::mutex> lg(set_blocked);
        close(client_socket);
        worker_descriptors.erase(client_socket);
    }

    cur_workers--;
    if (cur_workers == 0) {
        server_stop.notify_all();
    }

}






} // namespace MTblocking
} // namespace Network
} // namespace Afina
