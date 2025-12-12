#include <iostream>
#include <sys/socket.h>

#include <unistd.h>
#include <arpa/inet.h>
#include <thread>
#include <mutex>
#include <cstring>
#include <sstream>
#include <chrono>
#include "Database.h"

using namespace std;
using json = nlohmann::json;

const int PORT = 8080;
const int BUFFER_SIZE = 8192;
const int MAX_CLIENTS = 100;
const int SOCKET_TIMEOUT_SEC = 60;

mutex MapMutex;
map<string, unique_ptr<mutex>> databaseMutex;
mutex countMutex;

mutex& getDbMutex(const string& dbName) {
    lock_guard<mutex> lock(MapMutex);

    if (databaseMutex.find(dbName) == databaseMutex.end()) {
        databaseMutex[dbName] = make_unique<mutex>();
    }
    return *databaseMutex[dbName];
}

string threadIdToString(thread::id id) {
    stringstream ss;
    ss << id;
    return ss.str();
}

// Функция для установки таймаута на сокет
bool setSocketTimeout(int socket, int seconds) {
    timeval timeout;
    timeout.tv_sec = seconds;
    timeout.tv_usec = 0;

    if (setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        return false;
    }

    if (setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
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
                    lock_guard<mutex> lock(countMutex);
                    cout << "[-] Таймаут отправки данных клиенту" << endl;
                    return false;
                }
                continue;
            }

            lock_guard<mutex> lock(countMutex);
            cout << "[-] Ошибка отправки данных: " << strerror(errno) << endl;
            return false;
        }

        totalSent += sent;
    }

    return true;
}

void handleClient(int clientSocket, sockaddr_in clientAddress) {
    string threadId = threadIdToString(this_thread::get_id());

    {
        lock_guard<mutex> lock(countMutex);
        char clientIP[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
        cout << "[+] Клиент подключен: " << clientIP << ":"
                  << ntohs(clientAddress.sin_port)
                  << " (Поток: " << threadId << ")" << endl;
    }

    // Устанавливаем таймаут на сокет
    if (!setSocketTimeout(clientSocket, SOCKET_TIMEOUT_SEC)) {
        lock_guard<mutex> lock(countMutex);
        cout << "[-] Ошибка установки таймаута для клиента" << endl;
        close(clientSocket);
        return;
    }

    char buffer[BUFFER_SIZE];
    bool connectionAlive = true;

    while (connectionAlive) {
        HashMap map(3);
        memset(buffer, 0, BUFFER_SIZE);

        auto recvStart = chrono::steady_clock::now();
        int bytesRead = recv(clientSocket, buffer, BUFFER_SIZE - 1, 0);

        if (bytesRead < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                auto now = chrono::steady_clock::now();
                auto elapsed = chrono::duration_cast<chrono::seconds>(now - recvStart).count();

                if (elapsed >= SOCKET_TIMEOUT_SEC) {
                    lock_guard<mutex> lock(countMutex);
                    char clientIP[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
                    cout << "[-] Таймаут ожидания данных от клиента: " << clientIP << endl;
                }
                break;
            }

            lock_guard<mutex> lock(countMutex);
            char clientIP[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
            cout << "[-] Ошибка приема данных от клиента " << clientIP
                      << ": " << strerror(errno) << endl;
            break;
        }

        if (bytesRead == 0) {
            lock_guard<mutex> lock(countMutex);
            char clientIP[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
            cout << "[-] Клиент отключился корректно: " << clientIP << endl;
            break;
        }

        try {
            json inMsg = json::parse(buffer);
            string database = inMsg["database"];
            string collection = inMsg["collection"];
            string op = inMsg["operation"];

            {
                lock_guard<mutex> lock(countMutex);
                char clientIP[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
                cout << "[" << clientIP << ", поток: "<< threadId << "] " << "Получено: \n{" << endl;
                cout << "\t\"database\": " << database << endl;
                cout << "\t\"collection\": " << collection << endl;
                cout << "\t\"operation\": " << op << endl;
                if (op == "insert") {
                    cout << "\t\"data\": " << inMsg["data"].dump(15) << endl;
                } else {
                    cout << "\t\"query\": " << inMsg["query"].dump(10) << endl;
                }
                cout << "}" << endl;
            }

            filesystem::create_directories(database);
            string filename = database + "/" + collection + ".json";

            bool status = true;
            int inputCount;
            json data = json::array();

            json input;

            mutex& dbMutex = getDbMutex(database);
            lock_guard<mutex> db_lock(dbMutex);

            auto dbOperationStart = chrono::steady_clock::now();

            map.loadFromFile(filename);
            if (op == "insert") {
                if (Database::insertDoc(&map, inMsg["data"].dump())) {
                    map.saveToFile(filename);
                    status = true;
                } else {
                    status = false;
                }
            }
            else if (op == "find") {
                auto [count, docs] = Database::findDoc(&map, inMsg["query"].dump());
                if (count == 0) {
                    status = false;
                    input["message"] = "no documents found";
                }
                else {
                    status = true;
                    data = docs;
                    inputCount = count;
                    input["message"] = to_string(count) + " documents found";
                }
            } else if (op == "delete") {
                auto [count, docs] = Database::deleteDoc(&map, inMsg["query"].dump());
                if (count == 0) {
                    status = false;
                    input["message"] = "no documents to delete were found";
                }
                else {
                    status = true;
                    data = docs;
                    inputCount = count;
                    map.saveToFile(filename);
                    input["message"] = to_string(count) + " documents deleted";
                }
            }

            // Проверяем таймаут операции с БД
            auto dbOperationEnd = chrono::steady_clock::now();
            auto dbOperationDuration = chrono::duration_cast<chrono::seconds>(dbOperationEnd - dbOperationStart).count();
            if (dbOperationDuration > 5) {
                lock_guard<mutex> lock(countMutex);
                cout << "[!] Долгая операция с БД: " << dbOperationDuration << " сек" << endl;
            }

            input["status"] = status ? "success" : "error";
            if (!input.contains("message")) {
                input["message"] = status ? "operation is completed" : "operation failed";
            }
            if (op != "insert" && status) {
                input["data"] = data;
                input["count"] = inputCount;
            }
            string response = input.dump();

            if (!sendWithTimeout(clientSocket, response)) {
                connectionAlive = false;
            }
        } catch (const exception& e) {
            json errorResponse;
            errorResponse["status"] = "error";
            errorResponse["message"] = "JSON parsing error: " + string(e.what());
            string response = errorResponse.dump();

            if (!sendWithTimeout(clientSocket, response)) {
                connectionAlive = false;
            }
        }

        if (strcmp(buffer, "exit") == 0) {
            lock_guard<mutex> lock(countMutex);
            char clientIP[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
            cout << "[-] Клиент запросил выход: " << clientIP << endl;
            break;
        }
    }

    close(clientSocket);

    {
        lock_guard<mutex> lock(countMutex);
        cout << "[-] Поток " << threadId << " завершил обработку клиента" << endl;
    }
}

int main() {
    //создаём сокет
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket < 0) {
        cerr << "Ошибка создания сокета" << endl;
        return 1;
    }

    int opt = 1;
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        cerr << "Ошибка настройки сокета" << endl;
        close(serverSocket);
        return 1;
    }

    timeval timeout{};
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    if (setsockopt(serverSocket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        cerr << "Ошибка установки таймаута accept" << endl;
    }

    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(PORT);

    // Привязка
    if (bind(serverSocket, reinterpret_cast<sockaddr *>(&serverAddress), sizeof(serverAddress)) < 0) {
        cerr << "Ошибка привязки сокета" << endl;
        close(serverSocket);
        return 1;
    }

    if (listen(serverSocket, MAX_CLIENTS) < 0) {
        cerr << "Ошибка при прослушивании порта" << endl;
        close(serverSocket);
        return 1;
    }

    cout << "=== Сервер запущен на порту " << PORT << " ===" << endl;
    cout << "Таймаут сокета: " << SOCKET_TIMEOUT_SEC << " секунд" << endl;
    cout << "Ожидание подключений..." << endl;

    vector<thread> clientThreads;

    while (true) {
        sockaddr_in clientAddress{};
        socklen_t clientSize = sizeof(clientAddress);

        int clientSocket = accept(serverSocket, reinterpret_cast<struct sockaddr *>(&clientAddress), &clientSize);

        if (clientSocket < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Таймаут accept, продолжаем цикл
                continue;
            }
            cerr << "Ошибка при принятии подключения: " << strerror(errno) << endl;
            continue;
        }

        clientThreads.emplace_back(handleClient, clientSocket, clientAddress);

        clientThreads.back().detach();

        for (auto it = clientThreads.begin(); it != clientThreads.end(); ) {
            if (!it->joinable()) {
                it = clientThreads.erase(it);
            } else {
                ++it;
            }
        }

        {
            lock_guard<mutex> lock(countMutex);
            cout << "[Статистика] Активных потоков: " << clientThreads.size() << endl;
        }
    }
}