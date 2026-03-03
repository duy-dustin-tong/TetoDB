// server_main.cpp

#include <iostream>
#include "server/tetodb_instance.h"
#include "server/tcp_server.h"

using namespace tetodb;

int main() {
    std::cout << "===================================================\n";
    std::cout << "             TETODB SERVER KERNEL\n";
    std::cout << "===================================================\n";

    try {
        TetoDBInstance db("e2e.db");
        TcpServer server(&db, 5432); // Default PostgreSQL port

        server.Start();

        // Keep main thread alive until killed
        std::string cmd;
        while (std::cin >> cmd) {
            if (cmd == "shutdown") break;
        }

        server.Stop();
    }
    catch (const std::exception& e) {
        std::cerr << "Fatal Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}