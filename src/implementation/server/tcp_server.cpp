// tcp_server.cpp

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
#define SOCKET_ERROR -1
#endif

namespace tetodb {

// Safe recv wrapper — loops until exactly 'len' bytes are read.
// Returns false if the connection drops before all bytes arrive.
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
  std::cout << "[SERVER] Listening for TetoDB clients on port " << port_
            << "...\n";
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

// --- PgWire Helper Implementations ---
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

// --- UPDATED: Packet sender is now coalesced to prevent TCP fragmentation ---
void TcpServer::SendPacket(SOCKET sock, char type,
                           const std::vector<char> &payload) {
  std::vector<char> packet;
  if (type != 0)
    packet.push_back(type);

  uint32_t len = htonl(payload.size() + 4);
  const char *p = reinterpret_cast<const char *>(&len);
  packet.insert(packet.end(), p, p + 4);

  packet.insert(packet.end(), payload.begin(), payload.end());
  send(sock, packet.data(), packet.size(), 0);
}

void TcpServer::HandleClient(SOCKET client_socket) {
  ClientSession session;
  char buffer[4096];

  // =========================================================
  // 1. STARTUP NEGOTIATION
  // =========================================================
  if (!RecvAll(client_socket, buffer, 8)) {
    CLOSE_SOCKET(client_socket);
    return;
  }

  uint32_t raw_len, raw_protocol;
  std::memcpy(&raw_len, buffer, sizeof(uint32_t));
  std::memcpy(&raw_protocol, buffer + 4, sizeof(uint32_t));
  uint32_t len = ntohl(raw_len);
  uint32_t protocol = ntohl(raw_protocol);

  if (protocol == 80877103) {       // SSLRequest
    send(client_socket, "N", 1, 0); // Deny SSL

    if (!RecvAll(client_socket, buffer, 8)) {
      CLOSE_SOCKET(client_socket);
      return;
    } // Read real Startup header
    uint32_t raw_len_2;
    std::memcpy(&raw_len_2, buffer, sizeof(uint32_t));
    len = ntohl(raw_len_2);
  }

  // --- Cap startup packet length to prevent OOM ---
  static constexpr uint32_t MAX_STARTUP_LEN = 1024 * 1024; // 1 MB
  if (len > MAX_STARTUP_LEN) {
    CLOSE_SOCKET(client_socket);
    return;
  }
  if (len > 8) {
    std::vector<char> discard(len - 8);
    RecvAll(client_socket, discard.data(), len - 8);
  }

  // =========================================================
  // 2. THE HANDSHAKE
  // =========================================================
  std::vector<char> auth_ok;
  WriteInt32(auth_ok, 0);
  SendPacket(client_socket, 'R', auth_ok);

  auto send_param = [&](const std::string &k, const std::string &v) {
    std::vector<char> p;
    WriteString(p, k);
    WriteString(p, v);
    SendPacket(client_socket, 'S', p);
  };
  send_param("server_version", "14.0");
  send_param("server_encoding", "UTF8");
  send_param("client_encoding", "UTF8");
  send_param("DateStyle", "ISO, MDY");
  send_param("TimeZone", "UTC");

  std::vector<char> key_data;
  WriteInt32(key_data, 1234);
  WriteInt32(key_data, 5678);
  SendPacket(client_socket, 'K', key_data);

  std::vector<char> ready;
  WriteByte(ready, 'I');
  SendPacket(client_socket, 'Z', ready);

  // =========================================================
  // 3. COMMAND LOOP
  // =========================================================
  while (is_running_) {
    if (!RecvAll(client_socket, buffer, 5))
      break;

    char msg_type = buffer[0];
    uint32_t raw_msg_len;
    std::memcpy(&raw_msg_len, buffer + 1, sizeof(uint32_t));
    uint32_t msg_len = ntohl(raw_msg_len);

    // --- Cap query length to prevent OOM from malicious clients ---
    static constexpr uint32_t MAX_QUERY_LEN = 16 * 1024 * 1024; // 16 MB
    if (msg_len > MAX_QUERY_LEN) {
      std::cerr << "[SERVER] Rejecting oversized packet: " << msg_len
                << " bytes\n";
      break;
    }

    if (msg_type == 'X')
      break;

    if (msg_type == 'Q' || msg_type == 'E') {
      std::string sql;
      if (msg_type == 'Q') {
        std::vector<char> qb(msg_len - 4);
        if (!RecvAll(client_socket, qb.data(), msg_len - 4))
          break;
        sql = std::string(qb.data());
      } else {
        std::vector<char> pl(msg_len - 4);
        if (!RecvAll(client_socket, pl.data(), msg_len - 4))
          break;
        sql = session.prepared_statements[""];
      }

      if (!sql.empty()) {
        QueryResult res = db_->ExecuteQuery(sql, session);

        // --- RESTORED C++ SERVER LOGGING ---
        std::cout << "[SERVER] Executed: " << sql << "\n";
        if (res.is_error)
          std::cout << "[SERVER] Result: " << res.status_msg << "\n";
        // -----------------------------------

        if (res.is_error) {
          std::vector<char> err;
          WriteByte(err, 'S');
          WriteString(err, "ERROR");
          WriteByte(err, 'M');
          WriteString(err, res.status_msg);
          WriteByte(err, '\0');
          SendPacket(client_socket, 'E', err);
        } else {
          // 1. Send Row Description if there is a schema (Actual Columnar Data)
          if (res.schema) {
            std::vector<char> rd;
            WriteInt16(rd, res.schema->GetColumnCount());
            for (uint32_t i = 0; i < res.schema->GetColumnCount(); i++) {
              const auto &col = res.schema->GetColumn(i);
              WriteString(rd, col.GetName());
              WriteInt32(rd, 0);
              WriteInt16(rd, 0);
              int32_t pg_type = 25; // TEXT
              if (col.GetTypeId() == TypeId::INTEGER) {
                pg_type = 23; // INT4
              } else if (col.GetTypeId() == TypeId::BIGINT) {
                pg_type = 20; // INT8
              }
              WriteInt32(rd, pg_type);
              WriteInt16(rd, -1);
              WriteInt32(rd, -1);
              WriteInt16(rd, 0);
            }
            SendPacket(client_socket, 'T', rd);

            // 2. Send each Tuple as a DataRow
            for (const auto &tuple : res.rows) {
              std::vector<char> dr;
              WriteInt16(dr, res.schema->GetColumnCount());
              for (uint32_t i = 0; i < res.schema->GetColumnCount(); i++) {
                Value v = tuple.GetValue(res.schema, i);
                if (v.IsNull()) {
                  WriteInt32(dr, -1);
                } else {
                  std::string s = v.ToString();
                  WriteInt32(dr, s.length());
                  dr.insert(dr.end(), s.begin(), s.end());
                }
              }
              SendPacket(client_socket, 'D', dr);
            }
          }

          // 3. Command Complete
          std::vector<char> cc;
          WriteString(cc, res.status_msg);
          SendPacket(client_socket, 'C', cc);
        }
      }
      // ReadyForQuery is only sent after Simple Query ('Q').
      // For Extended Query Protocol ('E'), ReadyForQuery is sent by Sync ('S').
      if (msg_type == 'Q') {
        std::vector<char> z;
        WriteByte(z, session.active_txn ? 'T' : 'I');
        SendPacket(client_socket, 'Z', z);
      }
    } else if (msg_type == 'P') { // Parse
      std::vector<char> payload(msg_len - 4);
      if (!RecvAll(client_socket, payload.data(), msg_len - 4))
        break;

      const char *p = payload.data();
      std::string stmt_name(p);
      p += stmt_name.length() + 1;
      std::string query_str(p);

      session.prepared_statements[stmt_name] = query_str;
      SendPacket(client_socket, '1', {});
    } else if (msg_type == 'B') { // Bind
      std::vector<char> payload(msg_len - 4);
      if (!RecvAll(client_socket, payload.data(), msg_len - 4))
        break;

      const char *p = payload.data();
      std::string dest_portal(p);
      p += dest_portal.length() + 1;
      std::string stmt_name(p);
      p += stmt_name.length() + 1;

      // Read parameter format codes (0=text, 1=binary)
      int16_t raw_formats;
      std::memcpy(&raw_formats, p, sizeof(int16_t));
      int16_t num_formats = ntohs(raw_formats);
      p += 2;
      std::vector<int16_t> formats(num_formats);
      for (int i = 0; i < num_formats; i++) {
        int16_t format_val;
        std::memcpy(&format_val, p, sizeof(int16_t));
        formats[i] = ntohs(format_val);
        p += 2;
      }

      int16_t raw_params;
      std::memcpy(&raw_params, p, sizeof(int16_t));
      int16_t num_params = ntohs(raw_params);
      p += 2;

      session.current_parameters.clear();
      for (int i = 0; i < num_params; i++) {
        int32_t raw_param_len;
        std::memcpy(&raw_param_len, p, sizeof(int32_t));
        int32_t param_len = ntohl(raw_param_len);
        p += 4;
        if (param_len == -1) {
          session.current_parameters.push_back(
              Value::GetNullValue(TypeId::VARCHAR));
        } else {
          // Determine format: if only 1 format code, it applies to all params
          int16_t fmt = 0; // default text
          if (num_formats == 1) {
            fmt = formats[0];
          } else if (i < num_formats) {
            fmt = formats[i];
          }
          if (fmt == 1 && param_len == 4) {
            // Binary int32 (big-endian)
            uint32_t raw_val;
            std::memcpy(&raw_val, p, sizeof(uint32_t));
            int32_t val = ntohl(raw_val);
            p += param_len;
            session.current_parameters.push_back(Value(TypeId::INTEGER, val));
          } else if (fmt == 1 && param_len == 8) {
            // Binary int64 (big-endian)
            uint32_t high_raw, low_raw;
            std::memcpy(&high_raw, p, 4);
            std::memcpy(&low_raw, p + 4, 4);
            int64_t val = static_cast<int64_t>(
                (static_cast<uint64_t>(ntohl(high_raw)) << 32) |
                ntohl(low_raw));
            p += param_len;
            session.current_parameters.push_back(Value(TypeId::BIGINT, val));
          } else {
            // Text format — read as string
            std::string param_str(p, param_len);
            p += param_len;

            bool is_number =
                !param_str.empty() &&
                std::all_of(param_str.begin() + (param_str[0] == '-' ? 1 : 0),
                            param_str.end(), ::isdigit);
            if (is_number) {
              try {
                int64_t v = std::stoll(param_str);
                if (v >= INT32_MIN && v <= INT32_MAX) {
                  session.current_parameters.push_back(
                      Value(TypeId::INTEGER, static_cast<int32_t>(v)));
                } else {
                  session.current_parameters.push_back(
                      Value(TypeId::BIGINT, v));
                }
              } catch (...) {
                session.current_parameters.push_back(
                    Value(TypeId::VARCHAR, param_str));
              }
            } else {
              session.current_parameters.push_back(
                  Value(TypeId::VARCHAR, param_str));
            }
          }
        }
      }
      SendPacket(client_socket, '2', {});
    } else if (msg_type == 'D') { // Describe
      std::vector<char> payload(msg_len - 4);
      if (!RecvAll(client_socket, payload.data(), msg_len - 4))
        break;

      std::string sql = session.prepared_statements[""];
      std::string upper_sql = sql;
      std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(),
                     ::toupper);

      // --- THE FIX: Tell libpq if tuples are actually coming by testing the Query ---
      if (upper_sql.find("SELECT") != std::string::npos ||
          upper_sql.find("SHOW") != std::string::npos ||
          upper_sql.find("EXPLAIN") != std::string::npos) {
        
        // Execute the query to get the schema (but we might need to rollback or use a dry run. For now ExecuteQuery handles read-only SELECTs safely)
        QueryResult res = db_->ExecuteQuery(sql, session);
        
        if (res.schema) {
          std::vector<char> row_desc;
          WriteInt16(row_desc, res.schema->GetColumnCount());
          for (uint32_t i = 0; i < res.schema->GetColumnCount(); i++) {
            const auto &col = res.schema->GetColumn(i);
            WriteString(row_desc, col.GetName());
            WriteInt32(row_desc, 0); // Table OID (Not strictly needed for basic drivers)
            WriteInt16(row_desc, 0); // Column Index
            
            int32_t pg_type = 25; // TEXT
            if (col.GetTypeId() == TypeId::INTEGER) {
              pg_type = 23; // INT4
            } else if (col.GetTypeId() == TypeId::BIGINT) {
              pg_type = 20; // INT8
            }
            
            WriteInt32(row_desc, pg_type);
            WriteInt16(row_desc, -1); // Type size (-1 = varlena)
            WriteInt32(row_desc, -1); // Typemod
            WriteInt16(row_desc, 0);  // Format code (0 = text)
          }
          SendPacket(client_socket, 'T', row_desc);
        } else {
          SendPacket(client_socket, 'n', {});
        }
      } else {
        SendPacket(client_socket, 'n', {}); // NoData packet
      }
    } else if (msg_type == 'S') { // Sync
      std::vector<char> ready_query;
      WriteByte(ready_query, session.active_txn ? 'T' : 'I');
      SendPacket(client_socket, 'Z', ready_query);
    } else {
      std::vector<char> discard(msg_len - 4);
      RecvAll(client_socket, discard.data(), msg_len - 4);
    }
  }
  CLOSE_SOCKET(client_socket);
}
} // namespace tetodb