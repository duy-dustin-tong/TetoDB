// tcp_server.cpp
// TetoWire Protocol — lightweight binary protocol for TetoDB

#include "server/tcp_server.h"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#define CLOSE_SOCKET closesocket
#else
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#define CLOSE_SOCKET close
#endif

namespace tetodb {

// Safe recv wrapper — loops until exactly 'len' bytes are read.
static bool RecvAll(SOCKET sock, char *buf, int len) {
  int total = 0;
  while (total < len) {
    int n = recv(sock, buf + total, len - total, 0);
    if (n <= 0)
      return false;
    total += n;
  }
  return true;
}

TcpServer::TcpServer(TetoDBInstance *db, int port)
    : db_(db), port_(port), server_fd_(INVALID_SOCKET), is_running_(false) {}

TcpServer::~TcpServer() { Stop(); }

void TcpServer::Start() {
#ifdef _WIN32
  WSADATA wsaData;
  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    throw std::runtime_error("WSAStartup failed");
#endif

  server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd_ == INVALID_SOCKET)
    throw std::runtime_error("Socket creation failed");

  int opt = 1;
#ifdef _WIN32
  setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt,
             sizeof(opt));
#else
  setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
             sizeof(opt));
#endif

  struct sockaddr_in address;
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port_);

  if (bind(server_fd_, (struct sockaddr *)&address, sizeof(address)) ==
      SOCKET_ERROR) {
    throw std::runtime_error("Bind failed on port " + std::to_string(port_));
  }

  if (listen(server_fd_, 10) == SOCKET_ERROR)
    throw std::runtime_error("Listen failed");

  is_running_ = true;
  std::cout << "[SERVER] Listening on port " << port_ << " (TetoWire)...\n";
  accept_thread_ = std::thread(&TcpServer::AcceptLoop, this);
}

void TcpServer::Stop() {
  is_running_ = false;
  if (server_fd_ != INVALID_SOCKET) {
    CLOSE_SOCKET(server_fd_);
    server_fd_ = INVALID_SOCKET;
  }
  if (accept_thread_.joinable())
    accept_thread_.join();
  for (auto &t : client_threads_) {
    if (t.joinable())
      t.join();
  }
#ifdef _WIN32
  WSACleanup();
#endif
}

void TcpServer::AcceptLoop() {
  struct sockaddr_in client_addr;
  socklen_t client_len = sizeof(client_addr);
  while (is_running_) {
    SOCKET client_socket =
        accept(server_fd_, (struct sockaddr *)&client_addr, &client_len);
    if (client_socket == INVALID_SOCKET)
      continue;
    client_threads_.emplace_back(&TcpServer::HandleClient, this, client_socket);
  }
}

// --- TetoWire Byte Helpers ---
void TcpServer::WriteInt32(std::vector<char> &buf, int32_t val) {
  uint32_t net_val = htonl(val);
  const char *p = reinterpret_cast<const char *>(&net_val);
  buf.insert(buf.end(), p, p + 4);
}

void TcpServer::WriteInt16(std::vector<char> &buf, int16_t val) {
  uint16_t net_val = htons(val);
  const char *p = reinterpret_cast<const char *>(&net_val);
  buf.insert(buf.end(), p, p + 2);
}

void TcpServer::WriteString(std::vector<char> &buf, const std::string &str) {
  buf.insert(buf.end(), str.begin(), str.end());
  buf.push_back('\0');
}

void TcpServer::WriteByte(std::vector<char> &buf, char val) {
  buf.push_back(val);
}

void TcpServer::SendPacket(SOCKET sock, char type,
                           const std::vector<char> &payload) {
  std::vector<char> packet;
  packet.push_back(type);

  uint32_t len = htonl(static_cast<uint32_t>(payload.size() + 4));
  const char *p = reinterpret_cast<const char *>(&len);
  packet.insert(packet.end(), p, p + 4);

  packet.insert(packet.end(), payload.begin(), payload.end());
  send(sock, packet.data(), static_cast<int>(packet.size()), 0);
}

// =============================================================
// TetoWire HandleClient
// =============================================================
// Protocol:
//   Server → Client on connect: 'Z' (Ready, 1-byte txn state)
//   Client → Server: 'Q' [4-byte len] [SQL string\0]  or  'X' (disconnect)
//   Server → Client per query:
//     On success with rows: 'T' (schema) + N × 'D' (data rows) + 'C' (complete) + 'Z' (ready)
//     On success no rows:   'C' (complete) + 'Z' (ready)
//     On error:             'E' (error msg) + 'Z' (ready)
// =============================================================
void TcpServer::HandleClient(SOCKET client_socket) {
  ClientSession session;

  // Send initial ReadyForQuery
  {
    std::vector<char> z;
    WriteByte(z, 'I'); // Idle, no active transaction
    SendPacket(client_socket, 'Z', z);
  }

  // Command loop
  while (is_running_) {
    // Read 1-byte message type
    char msg_type;
    if (!RecvAll(client_socket, &msg_type, 1))
      break;

    if (msg_type == 'X') // Client disconnect
      break;

    if (msg_type == 'Q') {
      // Read 4-byte payload length (includes self)
      char len_buf[4];
      if (!RecvAll(client_socket, len_buf, 4))
        break;
      uint32_t raw_len;
      std::memcpy(&raw_len, len_buf, sizeof(uint32_t));
      uint32_t payload_len = ntohl(raw_len) - 4;

      // Cap query length
      static constexpr uint32_t MAX_QUERY_LEN = 16 * 1024 * 1024;
      if (payload_len > MAX_QUERY_LEN) {
        std::cerr << "[SERVER] Rejecting oversized query: " << payload_len
                  << " bytes\n";
        break;
      }

      // Read SQL payload
      std::vector<char> qb(payload_len);
      if (payload_len > 0 && !RecvAll(client_socket, qb.data(), payload_len))
        break;
      std::string sql(qb.data()); // null-terminated by client

      if (sql.empty()) {
        std::vector<char> z;
        WriteByte(z, session.active_txn ? 'T' : 'I');
        SendPacket(client_socket, 'Z', z);
        continue;
      }

      std::cout << "[SERVER] Executed: " << sql << "\n";

      // Execute query
      QueryResult res = db_->ExecuteQuery(sql, session);

      if (res.is_error) {
        std::cout << "[SERVER] Error: " << res.status_msg << "\n";
        // Send Error packet
        std::vector<char> err;
        WriteString(err, res.status_msg);
        SendPacket(client_socket, 'E', err);
      } else {
        // Send RowDescription if schema exists
        if (res.schema && res.schema->GetColumnCount() > 0) {
          std::vector<char> rd;
          uint32_t col_count = res.schema->GetColumnCount();
          WriteInt16(rd, static_cast<int16_t>(col_count));
          for (uint32_t i = 0; i < col_count; i++) {
            const auto &col = res.schema->GetColumn(i);
            WriteString(rd, col.GetName());

            // Send type name as string
            std::string type_name;
            switch (col.GetTypeId()) {
            case TypeId::BOOLEAN:
              type_name = "BOOLEAN";
              break;
            case TypeId::TINYINT:
              type_name = "TINYINT";
              break;
            case TypeId::SMALLINT:
              type_name = "SMALLINT";
              break;
            case TypeId::INTEGER:
              type_name = "INTEGER";
              break;
            case TypeId::BIGINT:
              type_name = "BIGINT";
              break;
            case TypeId::DECIMAL:
              type_name = "DECIMAL";
              break;
            case TypeId::VARCHAR:
              type_name = "VARCHAR";
              break;
            case TypeId::CHAR:
              type_name = "CHAR";
              break;
            case TypeId::TIMESTAMP:
              type_name = "TIMESTAMP";
              break;
            default:
              type_name = "VARCHAR";
              break;
            }
            WriteString(rd, type_name);
          }
          SendPacket(client_socket, 'T', rd);

          // Send each row as a DataRow
          for (const auto &tuple : res.rows) {
            std::vector<char> dr;
            WriteInt16(dr, static_cast<int16_t>(col_count));
            for (uint32_t i = 0; i < col_count; i++) {
              Value v = tuple.GetValue(res.schema, i);
              if (v.IsNull()) {
                WriteInt32(dr, -1); // NULL marker
              } else {
                std::string s = v.ToString();
                WriteInt32(dr, static_cast<int32_t>(s.length()));
                dr.insert(dr.end(), s.begin(), s.end());
              }
            }
            SendPacket(client_socket, 'D', dr);
          }
        }

        // Command Complete
        std::vector<char> cc;
        WriteString(cc, res.status_msg);
        SendPacket(client_socket, 'C', cc);
      }

      // ReadyForQuery
      std::vector<char> z;
      WriteByte(z, session.active_txn ? 'T' : 'I');
      SendPacket(client_socket, 'Z', z);
    } else {
      // Unknown message type — skip payload
      char len_buf[4];
      if (!RecvAll(client_socket, len_buf, 4))
        break;
      uint32_t raw_len;
      std::memcpy(&raw_len, len_buf, sizeof(uint32_t));
      uint32_t payload_len = ntohl(raw_len) - 4;
      if (payload_len > 0) {
        std::vector<char> discard(payload_len);
        RecvAll(client_socket, discard.data(), payload_len);
      }
    }
  }
  CLOSE_SOCKET(client_socket);
}

} // namespace tetodb