// tcp_server.h

#pragma once

#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include "server/tetodb_instance.h"

#ifdef _WIN32
#include <winsock2.h>
#else
typedef int SOCKET;
#define INVALID_SOCKET -1
#endif

namespace tetodb {

    class TcpServer {
    public:
        TcpServer(TetoDBInstance* db, int port = 5432);
        ~TcpServer();

        void Start();
        void Stop();

    private:
        void AcceptLoop();
        void HandleClient(SOCKET client_socket);

        // PgWire Byte Formatting Helpers
        void WriteInt32(std::vector<char>& buf, int32_t val);
        void WriteInt16(std::vector<char>& buf, int16_t val);
        void WriteString(std::vector<char>& buf, const std::string& str);
        void WriteByte(std::vector<char>& buf, char val);
        void SendPacket(SOCKET sock, char type, const std::vector<char>& payload);

        TetoDBInstance* db_;
        int port_;
        SOCKET server_fd_;
        std::atomic<bool> is_running_;
        std::vector<std::thread> client_threads_;
        std::thread accept_thread_;
    };

} // namespace tetodb