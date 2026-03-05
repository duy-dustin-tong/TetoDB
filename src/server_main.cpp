// server_main.cpp

#include "server/tcp_server.h"
#include "server/tetodb_instance.h"
#include <chrono>
#include <iostream>
#include <thread>

using namespace tetodb;

int main() {
  std::cout << "===================================================\n";
  std::cout << "             TETODB SERVER KERNEL\n";
  std::cout << "===================================================\n";

  try {
    TetoDBInstance db("e2e.db");
    TcpServer server(&db, 5432); // Default PostgreSQL port

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