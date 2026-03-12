// client_main.cpp
// TetoWire REPL Client for TetoDB

#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#define CLOSE_SOCKET closesocket
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#define CLOSE_SOCKET close
#define SOCKET_ERROR -1
typedef int SOCKET;
#define INVALID_SOCKET -1
#endif

// --- TetoWire Helpers ---

void WriteInt32(std::vector<char> &buf, int32_t val) {
  uint32_t net_val = htonl(val);
  const char *p = reinterpret_cast<const char *>(&net_val);
  buf.insert(buf.end(), p, p + 4);
}

void WriteString(std::vector<char> &buf, const std::string &str) {
  buf.insert(buf.end(), str.begin(), str.end());
  buf.push_back('\0');
}

bool RecvAll(SOCKET sock, char *buf, int len) {
  int total = 0;
  while (total < len) {
    int bytes = recv(sock, buf + total, len - total, 0);
    if (bytes <= 0)
      return false;
    total += bytes;
  }
  return true;
}

// Read a full TetoWire packet: [1-byte type] [4-byte len] [payload]
// Returns false if connection drops
bool RecvPacket(SOCKET sock, char &type, std::vector<char> &payload) {
  if (!RecvAll(sock, &type, 1))
    return false;
  char len_buf[4];
  if (!RecvAll(sock, len_buf, 4))
    return false;
  uint32_t raw_len;
  std::memcpy(&raw_len, len_buf, sizeof(uint32_t));
  uint32_t payload_len = ntohl(raw_len) - 4;
  payload.resize(payload_len);
  if (payload_len > 0 && !RecvAll(sock, payload.data(), payload_len))
    return false;
  return true;
}

uint16_t ReadInt16(const char *p) {
  uint16_t raw;
  std::memcpy(&raw, p, sizeof(uint16_t));
  return ntohs(raw);
}

int32_t ReadInt32(const char *p) {
  uint32_t raw;
  std::memcpy(&raw, p, sizeof(uint32_t));
  return static_cast<int32_t>(ntohl(raw));
}

int main() {
  std::cout << "Starting TetoDB Client (TetoWire)...\n";

#ifdef _WIN32
  WSADATA wsaData;
  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    return 1;
#endif

  SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock == INVALID_SOCKET)
    return 1;

  struct sockaddr_in serv_addr;
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(5432);

  if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0)
    return 1;
  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) ==
      SOCKET_ERROR) {
    std::cerr << "Connection Failed. Is TetoDB Server running?\n";
    return 1;
  }

  // Wait for initial ReadyForQuery
  char type;
  std::vector<char> payload;
  if (!RecvPacket(sock, type, payload) || type != 'Z') {
    std::cerr << "Failed to receive initial Ready signal.\n";
    return 1;
  }

  std::cout << "Connected to TetoDB.\n\n";

  // REPL loop
  std::string statement_buffer;
  while (true) {
    // Prompt
    if (statement_buffer.empty())
      std::cout << "tetodb> ";
    else
      std::cout << "      > ";

    std::string line_input;
    if (!std::getline(std::cin, line_input))
      break;

    if (statement_buffer.empty() &&
        (line_input == "exit" || line_input == "quit"))
      break;
    if (statement_buffer.empty() && line_input.empty())
      continue;

    statement_buffer += line_input + "\n";
    if (statement_buffer.find(';') == std::string::npos)
      continue;

    // Send Query packet: 'Q' [4-byte len] [sql\0]
    std::vector<char> q_packet;
    q_packet.push_back('Q');
    uint32_t sql_len = static_cast<uint32_t>(statement_buffer.length() + 1 + 4);
    WriteInt32(q_packet, sql_len);
    WriteString(q_packet, statement_buffer);
    send(sock, q_packet.data(), static_cast<int>(q_packet.size()), 0);

    statement_buffer.clear();

    // Read response packets until 'Z' (ReadyForQuery)
    std::vector<std::string> col_names;
    bool got_header = false;

    while (true) {
      char rtype;
      std::vector<char> rpayload;
      if (!RecvPacket(sock, rtype, rpayload)) {
        std::cerr << "Connection lost.\n";
        goto end_client;
      }

      if (rtype == 'T') {
        // RowDescription: [int16 col_count] [string name, string type] ...
        const char *p = rpayload.data();
        uint16_t col_count = ReadInt16(p);
        p += 2;
        col_names.clear();
        for (uint16_t i = 0; i < col_count; i++) {
          std::string name(p);
          p += name.length() + 1;
          std::string type_name(p); // skip type for display
          p += type_name.length() + 1;
          col_names.push_back(name);
        }
        // Print column header
        if (!col_names.empty()) {
          for (size_t i = 0; i < col_names.size(); i++) {
            if (i > 0) std::cout << " | ";
            std::cout << col_names[i];
          }
          std::cout << "\n";
          // Separator
          for (size_t i = 0; i < col_names.size(); i++) {
            if (i > 0) std::cout << "-+-";
            std::cout << std::string(col_names[i].length(), '-');
          }
          std::cout << "\n";
          got_header = true;
        }
      } else if (rtype == 'D') {
        // DataRow: [int16 col_count] [int32 len, bytes data] ...
        const char *p = rpayload.data();
        uint16_t col_count = ReadInt16(p);
        p += 2;
        for (uint16_t i = 0; i < col_count; i++) {
          if (i > 0) std::cout << " | ";
          int32_t col_len = ReadInt32(p);
          p += 4;
          if (col_len == -1) {
            std::cout << "NULL";
          } else if (col_len > 0) {
            std::cout << std::string(p, col_len);
            p += col_len;
          }
        }
        std::cout << "\n";
      } else if (rtype == 'C') {
        // CommandComplete
        std::string status(rpayload.data());
        if (got_header)
          std::cout << "\n";
        std::cout << status << "\n";
      } else if (rtype == 'E') {
        // Error
        std::string err(rpayload.data());
        std::cout << "ERROR: " << err << "\n";
      } else if (rtype == 'Z') {
        // ReadyForQuery — done with this query
        break;
      }
    }
  }

end_client:
  // Send disconnect
  {
    std::vector<char> x_packet;
    x_packet.push_back('X');
    WriteInt32(x_packet, 4);
    send(sock, x_packet.data(), static_cast<int>(x_packet.size()), 0);
  }

  CLOSE_SOCKET(sock);
#ifdef _WIN32
  WSACleanup();
#endif
  return 0;
}