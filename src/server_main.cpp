// server_main.cpp

#include "server/tcp_server.h"
#include "server/tetodb_instance.h"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

using namespace tetodb;

int main(int argc, char *argv[]) {
  std::string db_name = "mydb";
  int port = 5432;

  // Parse command-line args: teto_main [db_name] [port]
  if (argc >= 2)
    db_name = argv[1];
  if (argc >= 3)
    port = std::stoi(argv[2]);

  std::cout << "===================================================\n";
  std::cout << "             TETODB SERVER KERNEL\n";
  std::cout << "===================================================\n";
  std::cout << "Database: " << db_name << "  |  Port: " << port << "\n\n";

  try {
    // Create a dedicated directory for this database
    std::filesystem::path db_dir = "data_" + db_name;
    std::filesystem::create_directories(db_dir);

    // All files go inside: data_<name>/<name>.db, .log, .freelist, .catalog
    std::string db_file = (db_dir / (db_name + ".db")).string();

    TetoDBInstance db(db_file);
    TcpServer server(&db, port);

    server.Start();

    // Interactive mode: read stdin for "shutdown" command
    std::string cmd;
    while (std::cin >> cmd) {
      if (cmd == "shutdown")
        break;
    }

    // If stdin was closed (headless / background), block until killed
    if (!std::cin.good()) {
      while (true) {
        std::this_thread::sleep_for(std::chrono::hours(24));
      }
    }

    server.Stop();
  } catch (const std::exception &e) {
    std::cerr << "Fatal Error: " << e.what() << "\n";
    return 1;
  }

  return 0;
}