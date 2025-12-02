#include <iostream>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>


using namespace std;

string host = "127.0.0.1";
int port = 8080;
string database;


int main(int argc, char* argv[]) {
    //Парсим строчку на подключение
    for (int i = 1; i < argc; i += 2) {
        string arg = argv[i];
        if (arg == "--host" && i+1 < argc) host = argv[i+1];
        else if (arg == "--port" && i+1 < argc) port = stoi(argv[i+1]);
        else if (arg == "--database" && i+1 < argc) database = argv[i+1];
    }
    if (database.empty()) {
        cerr << "Usage: client --host 127.0.0.1 --port 8080 --database mydb\n";
        return 1;
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in serv{};
    serv.sin_family = AF_INET;
    serv.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &serv.sin_addr);

    if (connect(sock, reinterpret_cast<struct sockaddr *>(&serv), sizeof(serv)) < 0) {
        perror("connect");
        return 1;
    }

    cout << "Подключено к " << host << ":" << port << ", база: " << database << endl;

    string line;
    while (cout << "> " && getline(cin, line)) {
        if (line == "exit" || line == "quit") break;
        if (line.empty()) continue;

        try {
            send(sock, line.c_str(), line.size(), 0);

            char buf[16384] = {0};
        } catch (const exception& e) {
            cout << "Ошибка: " << e.what() << endl;
        }
    }

    close(sock);
    return 0;
}