#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <thread>
#include <mutex>
#include <cstring>
#include <sstream>
#include "Database.h"

const int PORT = 8080;
const int BUFFER_SIZE = 8192;
const int MAX_CLIENTS = 100;

std::mutex MapMutex;
std::map<std::string, std::unique_ptr<std::mutex>> databaseMutex;

std::mutex cout_mutex;

std::mutex& getDbMutex(const std::string& dbName) {
    std::lock_guard<std::mutex> lock(MapMutex);

    if (databaseMutex.find(dbName) == databaseMutex.end()) {
        databaseMutex[dbName] = std::make_unique<std::mutex>();
    }
    return *databaseMutex[dbName];
}

std::string thread_id_to_string(std::thread::id id) {
    std::stringstream ss;
    ss << id;
    return ss.str();
}

void handleClient(int clientSocket, sockaddr_in clientAddress) {
    std::string threadId = thread_id_to_string(std::this_thread::get_id());

    {
        std::lock_guard<std::mutex> lock(cout_mutex);
        char clientIP[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
        std::cout << "[+] Клиент подключен: " << clientIP << ":"
                  << ntohs(clientAddress.sin_port)
                  << " (Поток: " << threadId << ")" << std::endl;
    }

    char buffer[BUFFER_SIZE];

    while (true) {
        HashMap map(3);
        memset(buffer, 0, BUFFER_SIZE);
        int bytesRead = recv(clientSocket, buffer, BUFFER_SIZE - 1, 0);

        if (bytesRead <= 0) {
            if (bytesRead == 0) {
                // Клиент корректно отключился
                std::lock_guard<std::mutex> lock(cout_mutex);
                char clientIP[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
                std::cout << "[-] Клиент отключился корректно: " << clientIP << std::endl;
            }
            break;
        }
        try {
            nlohmann::json inMsg = nlohmann::json::parse(buffer);
            std::string database = inMsg["database"];
            std::string collection = inMsg["collection"];
            std::string op = inMsg["operation"];

            {
                std::lock_guard<std::mutex> lock(cout_mutex);
                char clientIP[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
                std::cout << "[" << clientIP << ", поток: "<< threadId << "] " << "Получено: \n{" << std::endl;
                std::cout << "\t\"database\": " << database << std::endl;
                std::cout << "\t\"collection\": " << collection << std::endl;
                std::cout << "\t\"operation\": " << op << std::endl;
                if (op == "insert") {
                    std::cout << "\t\"data\": " << inMsg["data"].dump(15) << std::endl;
                } else {
                    std::cout << "\t\"query\": " << inMsg["query"].dump(10) << std::endl;
                }
                std::cout << "}" << std::endl;
            }


            std::filesystem::create_directories(database);
            std::string filename = database + "/" + collection + ".json";

            bool status = true;
            int inputCount;
            nlohmann::json data = nlohmann::json::array();

            nlohmann::json input;

            std::mutex& dbMutex = getDbMutex(database);
            std::lock_guard<std::mutex> db_lock(dbMutex);
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
                    input["massage"] = "no documents found";
                }
                else {
                    status = true;
                    data = docs;
                    inputCount = count;
                    input["massage"] = std::to_string(count) + " documents found";
                }
            } else if (op == "delete") {
                auto [count, docs] = Database::deleteDoc(&map, inMsg["query"].dump());
                if (count == 0) {
                    status = false;
                    input["massage"] = "no documents to delete were found";
                }
                else {
                    status = true;
                    data = docs;
                    inputCount = count;
                    map.saveToFile(filename);
                    input["massage"] = std::to_string(count) + " documents deleted";
                }
            }
            input["status"] = status ? "success" : "error";
            if (!input.contains("message")) {
                input["message"] = status ? "operation is completed" : "operation failed";
            }
            if (op != "insert" && status) {
                input["data"] = data;
                input["count"] = inputCount;
            }
            std::string response = input.dump();

            send(clientSocket, response.c_str(), response.length(), 0);
        } catch (const std::exception& e) {
            nlohmann::json errorResponse;
            errorResponse["status"] = "error";
            errorResponse["message"] = "JSON parsing error: " + std::string(e.what());
            std::string response = errorResponse.dump();
            send(clientSocket, response.c_str(), response.length(), 0);
        }


        if (strcmp(buffer, "exit") == 0) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            char clientIP[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &clientAddress.sin_addr, clientIP, INET_ADDRSTRLEN);
            std::cout << "[-] Клиент запросил выход: " << clientIP << std::endl;
            break;
        }
    }

    close(clientSocket);

    {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << "[-] Поток " << threadId << " завершил обработку клиента" << std::endl;
    }
}

int main() {
    // Создание сокета
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket < 0) {
        std::cerr << "Ошибка создания сокета" << std::endl;
        return 1;
    }

    // Опции для повторного использования порта
    int opt = 1;
    if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "Ошибка настройки сокета" << std::endl;
        close(serverSocket);
        return 1;
    }

    // Настройка адреса
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(PORT);

    // Привязка
    if (bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0) {
        std::cerr << "Ошибка привязки сокета" << std::endl;
        close(serverSocket);
        return 1;
    }

    // Прослушивание
    if (listen(serverSocket, MAX_CLIENTS) < 0) {
        std::cerr << "Ошибка при прослушивании порта" << std::endl;
        close(serverSocket);
        return 1;
    }

    std::cout << "=== Многопоточный сервер запущен на порту " << PORT << " ===" << std::endl;
    std::cout << "Ожидание подключений..." << std::endl;

    std::vector<std::thread> clientThreads;

    while (true) {
        // Принятие подключения
        sockaddr_in clientAddress{};
        socklen_t clientSize = sizeof(clientAddress);
        int clientSocket = accept(serverSocket, reinterpret_cast<struct sockaddr *>(&clientAddress), &clientSize);

        if (clientSocket < 0) {
            std::cerr << "Ошибка при принятии подключения" << std::endl;
            continue;
        }

        // Создание потока для обработки клиента
        clientThreads.emplace_back(handleClient, clientSocket, clientAddress);

        // Отсоединяем поток, чтобы не ждать его завершения
        clientThreads.back().detach();

        // Очистка завершенных потоков (упрощенная версия)
        for (auto it = clientThreads.begin(); it != clientThreads.end(); ) {
            if (!it->joinable()) {
                it = clientThreads.erase(it);
            } else {
                ++it;
            }
        }

        // Покажем количество активных потоков
        {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "[Статистика] Активных потоков: " << clientThreads.size() << std::endl;
        }
    }
}