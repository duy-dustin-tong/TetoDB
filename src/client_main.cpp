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
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#define CLOSE_SOCKET close
#define SOCKET_ERROR -1
typedef int SOCKET;
#define INVALID_SOCKET -1
#endif

// PgWire Byte Formatting Helpers
void WriteInt32(std::vector<char> &buf, int32_t val) {
  uint32_t net_val = htonl(val);
  const char *p = reinterpret_cast<const char *>(&net_val);
  buf.insert(buf.end(), p, p + 4);
}

void WriteString(std::vector<char> &buf, const std::string &str) {
  buf.insert(buf.end(), str.begin(), str.end());
  buf.push_back('\0');
}

// Helper to ensure we read exactly the required bytes
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

int main() {
  std::cout << "Starting TetoDB PgWire Client...\n";

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
  startup_packet.insert(startup_packet.end(), startup_payload.begin(),
                        startup_payload.end());

  send(sock, startup_packet.data(), startup_packet.size(), 0);

  // 2. Message Loop
  char type_byte;
  std::string statement_buffer;

  while (recv(sock, &type_byte, 1, 0) > 0) {
    char len_buf[4];
    if (!RecvAll(sock, len_buf, 4))
      break;
    uint32_t raw_len;
    std::memcpy(&raw_len, len_buf, sizeof(uint32_t));
    uint32_t len = ntohl(raw_len) - 4;

    std::vector<char> payload(len);
    if (len > 0 && !RecvAll(sock, payload.data(), len))
      break;

    if (type_byte == 'Z') { // ReadyForQuery
      // Prompt user for input
      while (true) {
        if (statement_buffer.empty())
          std::cout << "tetodb> ";
        else
          std::cout << "      > ";

        std::string line_input;
        if (!std::getline(std::cin, line_input))
          goto end_client;

        if (statement_buffer.empty() &&
            (line_input == "exit" || line_input == "quit"))
          goto end_client;
        if (statement_buffer.empty() && line_input.empty())
          continue;

        statement_buffer += line_input + "\n";
        if (statement_buffer.find(';') != std::string::npos)
          break;
      }

      // Send Simple Query ('Q')
      std::vector<char> q_packet;
      q_packet.push_back('Q');
      WriteInt32(q_packet, statement_buffer.length() + 1 + 4);
      WriteString(q_packet, statement_buffer);
      send(sock, q_packet.data(), q_packet.size(), 0);

      statement_buffer.clear();
    } else if (type_byte == 'D') { // DataRow
      // Unpack all columns in the DataRow
      if (len >= 2) {
        uint16_t raw_num_cols;
        std::memcpy(&raw_num_cols, payload.data(), sizeof(uint16_t));
        uint16_t num_cols = ntohs(raw_num_cols);
        uint32_t offset = 2;
        for (uint16_t i = 0; i < num_cols; i++) {
          if (offset + 4 > len)
            break;
          uint32_t raw_col_len;
          std::memcpy(&raw_col_len, payload.data() + offset, sizeof(uint32_t));
          uint32_t col_len = ntohl(raw_col_len);
          offset += 4;
          if (col_len == 0xFFFFFFFF) {
            std::cout << "NULL";
          } else if (col_len > 0) {
            if (offset + col_len > len)
              break;
            std::cout << std::string(payload.data() + offset, col_len);
            offset += col_len;
          }
          if (i < num_cols - 1)
            std::cout << " | ";
        }
        std::cout << "\n";
      }
    } else if (type_byte == 'C') { // CommandComplete
      std::cout << std::string(payload.data()) << "\n";
    } else if (type_byte == 'E') { // ErrorResponse
      std::cout << "Server Error: ";
      // The 'E' payload consists of one or more fields of format: [FieldType:1
      // char][String:NULL string] We'll just look for the 'M' (Message) field
      // or print raw if not formatted well.
      size_t i = 0;
      bool found_msg = false;
      while (i < payload.size()) {
        char field_type = payload[i++];
        if (field_type == '\0')
          break;
        std::string field_str(payload.data() + i);
        if (field_type == 'M') {
          std::cout << field_str;
          found_msg = true;
          break;
        }
        i += field_str.length() + 1;
      }
      if (!found_msg)
        std::cout << std::string(payload.data());
      std::cout << "\n";
    }
    // Ignores 'R' (Authentication), 'T' (RowDescription)
  }

end_client:
  CLOSE_SOCKET(sock);
#ifdef _WIN32
  WSACleanup();
#endif
  return 0;
}