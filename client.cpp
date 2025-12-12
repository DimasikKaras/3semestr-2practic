#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <chrono>
#include <sstream>
#include "nlohmann/json.hpp"

using namespace std;
using json = nlohmann::json;

int PORT;
constexpr int BUFFER_SIZE = 8192;
constexpr int SOCKET_TIMEOUT_SEC = 10;
string SERVERIP;
string nameDatabase;

bool isValidIP(const string& ip) {
    sockaddr_in sa;
    return inet_pton(AF_INET, ip.c_str(), &(sa.sin_addr)) == 1;
}

bool setSocketTimeout(int socket, int seconds) {
    timeval timeout;
    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    if (setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        cerr << "Ошибка установки таймаута приема: " << strerror(errno) << endl;
        return false;
    }

    if (setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
        cerr << "Ошибка установки таймаута отправки: " << strerror(errno) << endl;
        return false;
    }

    return true;
}

// Функция для отправки данных с таймаутом
bool sendWithTimeout(int socket, const string& data) {
    const char* buffer = data.c_str();
    size_t totalSent = 0;
    size_t dataSize = data.length();

    auto start = chrono::steady_clock::now();

    while (totalSent < dataSize) {
        int sent = send(socket, buffer + totalSent, dataSize - totalSent, 0);

        if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Таймаут отправки
                auto now = chrono::steady_clock::now();
                auto elapsed = chrono::duration_cast<chrono::seconds>(now - start).count();

                if (elapsed >= SOCKET_TIMEOUT_SEC) {
                    cerr << "[-] Таймаут отправки данных на сервер" << endl;
                    return false;
                }
                continue;
            }

            // Другая ошибка
            cerr << "[-] Ошибка отправки данных: " << strerror(errno) << endl;
            return false;
        }

        totalSent += sent;
    }

    return true;
}

// Функция для приема данных с таймаутом
string receiveWithTimeout(int socket, char* buffer, int bufferSize) {
    memset(buffer, 0, bufferSize);

    auto start = chrono::steady_clock::now();

    while (true) {
        int bytesRead = recv(socket, buffer, bufferSize - 1, 0);

        if (bytesRead < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                auto now = chrono::steady_clock::now();
                auto elapsed = chrono::duration_cast<chrono::seconds>(now - start).count();

                if (elapsed >= SOCKET_TIMEOUT_SEC) {
                    cerr << "[-] Таймаут ожидания ответа от сервера" << endl;
                    return "";
                }
                continue;
            }

            // Другая ошибка
            cerr << "[-] Ошибка получения данных: " << strerror(errno) << endl;
            return "";
        }

        if (bytesRead == 0) {
            cerr << "[-] Сервер отключился" << endl;
            return "";
        }

        return string(buffer, bytesRead);
    }
}

int main(int argv, char* argc[]) {
    try {
        if (argv < 7) {
            cerr << "Использование: " << argc[0] << " --host <IP> --port <PORT> --database <DB_NAME>" << endl;
            cerr << "Пример: " << argc[0] << " --host localhost --port 8080 --database mydb" << endl;
            return 1;
        }

        int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (clientSocket < 0) {
            cerr << "Ошибка создания сокета: " << strerror(errno) << endl;
            return 1;
        }

        if (!setSocketTimeout(clientSocket, SOCKET_TIMEOUT_SEC)) {
            cerr << "Не удалось установить таймаут на сокет" << endl;
            close(clientSocket);
            return 1;
        }

        if (string(argc[1]) == "--host" && !string(argc[2]).empty()) {
            if (string(argc[2]) == "localhost") {
                SERVERIP = "127.0.0.1";
            } else {
                if (isValidIP(argc[2])) {
                    SERVERIP = argc[2];
                } else {
                    cerr << "Неверный IP-адрес: " << argc[2] << endl;
                    close(clientSocket);
                    return 1;
                }
            }
        } else {
            cerr << "Не указан хост (--host)" << endl;
            close(clientSocket);
            return 1;
        }

        if (string(argc[3]) == "--port" && !string(argc[4]).empty()) {
            int port = stoi(argc[4]);
            if (port >= 1024 && port <= 65535) {
                PORT = port;
            } else {
                cerr << "Неверный порт: " << port << " (должен быть 1024-65535)" << endl;
                close(clientSocket);
                return 1;
            }
        } else {
            cerr << "Не указан порт (--port)" << endl;
            close(clientSocket);
            return 1;
        }

        if (string(argc[5]) == "--database" && !string(argc[6]).empty()) {
            nameDatabase = argc[6];
        } else {
            cerr << "Не указана база данных (--database)" << endl;
            close(clientSocket);
            return 1;
        }

        // Настройка адреса сервера
        sockaddr_in serverAddress{};
        serverAddress.sin_family = AF_INET;
        serverAddress.sin_port = htons(PORT);

        if (inet_pton(AF_INET, SERVERIP.c_str(), &serverAddress.sin_addr) <= 0) {
            cerr << "Неверный адрес сервера: " << SERVERIP << endl;
            close(clientSocket);
            return 1;
        }

        // Подключение к серверу с таймаутом
        cout << "Подключение к серверу " << SERVERIP << ":" << PORT << "..." << endl;

        auto connectStart = chrono::steady_clock::now();

        while (true) {
            int connectResult = connect(clientSocket, reinterpret_cast<sockaddr *>(&serverAddress), sizeof(serverAddress));

            if (connectResult == 0) {
                break;
            }

            if (errno == EINPROGRESS || errno == EALREADY) {
                // Подключение в процессе
                auto now = chrono::steady_clock::now();
                auto elapsed = chrono::duration_cast<chrono::seconds>(now - connectStart).count();

                if (elapsed >= SOCKET_TIMEOUT_SEC) {
                    cerr << "Таймаут подключения к серверу" << endl;
                    close(clientSocket);
                    return 1;
                }

                // Ждем немного перед повторной попыткой
                sleep(1);
                continue;
            }

            // Другая ошибка
            cerr << "Ошибка подключения к серверу: " << strerror(errno) << endl;
            close(clientSocket);
            return 1;
        }

        cout << "Успешно подключено к серверу " << SERVERIP << ":" << PORT << endl;
        cout << "База данных: " << nameDatabase << endl;
        cout << "Таймаут операций: " << SOCKET_TIMEOUT_SEC << " секунд" << endl;
        cout << "Введите команды (INSERT, FIND, DELETE) или 'exit' для выхода:" << endl;

        char buffer[BUFFER_SIZE];
        string message;

        while (true) {
            cout << "> ";
            getline(cin, message);

            if (message == "exit") {
                cout << "Завершение работы клиента" << endl;
                break;
            }
            if (message.empty()) continue;

            istringstream iss(message);
            string cmd, collection, jsonPart;
            iss >> cmd >> collection;
            getline(iss, jsonPart);

            jsonPart.erase(0, jsonPart.find_first_not_of(" "));

            json msg;
            msg["database"] = nameDatabase;
            msg["collection"] = collection;

            try {
                if (cmd == "INSERT") {
                    msg["operation"] = "insert";
                    msg["data"] = json::parse(jsonPart);
                } else if (cmd == "FIND") {
                    msg["operation"] = "find";
                    msg["query"] = json::parse(jsonPart);
                } else if (cmd == "DELETE") {
                    msg["operation"] = "delete";
                    msg["query"] = json::parse(jsonPart);
                } else {
                    cout << "Неизвестная команда: " << cmd << endl;
                    cout << "Доступные команды: INSERT, FIND, DELETE" << endl;
                    continue;
                }
            } catch (const exception& e) {
                cout << "Ошибка парсинга JSON: " << e.what() << endl;
                continue;
            }

            message = msg.dump();

            // Отправляем запрос с таймаутом
            if (!sendWithTimeout(clientSocket, message)) {
                cerr << "Не удалось отправить запрос на сервер" << endl;
                break;
            }

            string response = receiveWithTimeout(clientSocket, buffer, BUFFER_SIZE);

            if (response.empty()) {
                cerr << "Не удалось получить ответ от сервера" << endl;
                break;
            }

            try {
                cout << "Ответ сервера: " << json::parse(response).dump(4) << endl;
            } catch (const exception& e) {
                cout << "Ответ сервера (не JSON): " << response << endl;
                cout << "Ошибка парсинга JSON ответа: " << e.what() << endl;
            }
        }
        close(clientSocket);
        cout << "Клиент завершил работу" << endl;
        return 0;

    } catch (const exception& e) {
        cerr << "Ошибка: " << e.what() << endl;
        return 1;
    } catch (...) {
        cerr << "Неизвестная ошибка" << endl;
        return 1;
    }
}