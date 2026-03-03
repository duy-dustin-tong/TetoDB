// client_main.cpp

// client_main.cpp
#include <iostream>
#include <string>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#define CLOSE_SOCKET closesocket
#else
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#define CLOSE_SOCKET close
#define SOCKET_ERROR -1
typedef int SOCKET;
#define INVALID_SOCKET -1
#endif

// PgWire Byte Formatting Helpers
void WriteInt32(std::vector<char>& buf, int32_t val) {
    uint32_t net_val = htonl(val);
    const char* p = reinterpret_cast<const char*>(&net_val);
    buf.insert(buf.end(), p, p + 4);
}

void WriteString(std::vector<char>& buf, const std::string& str) {
    buf.insert(buf.end(), str.begin(), str.end());
    buf.push_back('\0');
}

// Helper to ensure we read exactly the required bytes
bool RecvAll(SOCKET sock, char* buf, int len) {
    int total = 0;
    while (total < len) {
        int bytes = recv(sock, buf + total, len - total, 0);
        if (bytes <= 0) return false;
        total += bytes;
    }
    return true;
}

int main() {
    std::cout << "Starting TetoDB PgWire Client...\n";

#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) return 1;
#endif

    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) return 1;

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(5432);

    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) return 1;
    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == SOCKET_ERROR) {
        std::cerr << "Connection Failed. Is TetoDB Server running?\n";
        return 1;
    }

    // 1. Send StartupMessage (PgWire v3.0)
    std::vector<char> startup_payload;
    WriteInt32(startup_payload, 196608); // Protocol Version 3.0
    WriteString(startup_payload, "user");
    WriteString(startup_payload, "postgres");
    WriteString(startup_payload, "database");
    WriteString(startup_payload, "postgres");
    startup_payload.push_back('\0');

    std::vector<char> startup_packet;
    WriteInt32(startup_packet, startup_payload.size() + 4);
    startup_packet.insert(startup_packet.end(), startup_payload.begin(), startup_payload.end());

    send(sock, startup_packet.data(), startup_packet.size(), 0);

    // 2. Message Loop
    char type_byte;
    std::string statement_buffer;

    while (recv(sock, &type_byte, 1, 0) > 0) {
        char len_buf[4];
        if (!RecvAll(sock, len_buf, 4)) break;
        uint32_t len = ntohl(*reinterpret_cast<uint32_t*>(len_buf)) - 4;

        std::vector<char> payload(len);
        if (len > 0 && !RecvAll(sock, payload.data(), len)) break;

        if (type_byte == 'Z') { // ReadyForQuery
            // Prompt user for input
            while (true) {
                if (statement_buffer.empty()) std::cout << "tetodb> ";
                else std::cout << "      > ";

                std::string line_input;
                if (!std::getline(std::cin, line_input)) goto end_client;

                if (statement_buffer.empty() && (line_input == "exit" || line_input == "quit")) goto end_client;
                if (statement_buffer.empty() && line_input.empty()) continue;

                statement_buffer += line_input + "\n";
                if (statement_buffer.find(';') != std::string::npos) break;
            }

            // Send Simple Query ('Q')
            std::vector<char> q_packet;
            q_packet.push_back('Q');
            WriteInt32(q_packet, statement_buffer.length() + 1 + 4);
            WriteString(q_packet, statement_buffer);
            send(sock, q_packet.data(), q_packet.size(), 0);

            statement_buffer.clear();
        }
        else if (type_byte == 'D') { // DataRow
            // Unpack the single text column sent by our server
            if (len >= 6) {
                uint32_t col_len = ntohl(*reinterpret_cast<uint32_t*>(payload.data() + 2));
                if (col_len > 0 && col_len != 0xFFFFFFFF) {
                    std::cout << std::string(payload.data() + 6, col_len);
                }
            }
        }
        else if (type_byte == 'E') { // ErrorResponse
            std::cout << "Server Error: " << std::string(payload.data()) << "\n";
        }
        // Ignores 'R' (Authentication), 'T' (RowDescription), 'C' (CommandComplete)
    }

end_client:
    CLOSE_SOCKET(sock);
#ifdef _WIN32
    WSACleanup();
#endif
    return 0;
}