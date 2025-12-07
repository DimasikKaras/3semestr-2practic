#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include "nlohmann/json.hpp"

int PORT;
constexpr int BUFFER_SIZE = 8192;
std::string SERVER_IP;
std::string nameDatabase;

bool isValidIP(const std::string& ip) {
    sockaddr_in sa{};
    return inet_pton(AF_INET, ip.c_str(), &(sa.sin_addr)) == 1;
}

int main(const int argv, char* argc[]) {
    try {
        if (argv < 7) throw;
        const int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (clientSocket < 0) {
            std::cerr << "Ошибка создания сокета" << std::endl;
        }


        if (std::string(argc[1]) == "--host" && !std::string(argc[2]).empty()) {
            if (std::string(argc[2]) == "localhost") SERVER_IP = "127.0.0.1";
            else {
                if (isValidIP(argc[2])) SERVER_IP = argc[2];
                else throw;
            }
        }

        if (std::string(argc[3]) == "--port" && !std::string(argc[4]).empty()) {
            if (std::stoi(argc[4]) >= 1024 && std::stoi(argc[4]) <= 65535) PORT = std::stoi(argc[4]);
            else throw;
        }

        if (std::string(argc[5]) == "--database" && !std::string(argc[6]).empty()) {
            nameDatabase = argc[6];
        }

        // Настройка адреса сервера
        sockaddr_in serverAddress{};
        serverAddress.sin_family = AF_INET;
        serverAddress.sin_port = htons(PORT);

        if (inet_pton(AF_INET, SERVER_IP.c_str(), &serverAddress.sin_addr) <= 0) {
            std::cerr << "Неверный адрес сервера" << std::endl;
            close(clientSocket);
            return 1;
        }

        // Подключение к серверу
        if (connect(clientSocket, reinterpret_cast<sockaddr *>(&serverAddress), sizeof(serverAddress)) < 0) {
            std::cerr << "Ошибка подключения к серверу" << std::endl;
            close(clientSocket);
            return 1;
        }

        std::cout << "Подключено к серверу " << SERVER_IP << ":" << PORT << std::endl;
        std::cout << "Введите сообщение (или 'exit' для выхода):" << std::endl;

        char buffer[BUFFER_SIZE];
        std::string message;

        while (true) {
            std::cout << "> ";
            std::getline(std::cin, message);

            if (message == "exit") {
                std::cout << "Завершение работы клиента" << std::endl;
                break;
            }
            if (message.empty()) continue;

            std::istringstream iss(message);
            std::string cmd, collection, jsonPart;
            iss >> cmd >> collection;
            std::getline(iss, jsonPart);
            nlohmann::json msg;
            msg["database"] = nameDatabase;
            msg["collection"] = collection;

            if (cmd == "INSERT") {
                msg["operation"] = "insert";
                msg["data"] = nlohmann::json::parse(jsonPart);
            } else if (cmd == "FIND") {
                msg["operation"] = "find";
                msg["query"] = nlohmann::json::parse(jsonPart);
            } else if (cmd == "DELETE") {
                msg["operation"] = "delete";
                msg["query"] = nlohmann::json::parse(jsonPart);
            } else {
                std::cout << "Invalid query" << std::endl;
                continue;
            }

            message = msg.dump(4);
            send(clientSocket, message.c_str(), message.length(), 0);
            // Получение ответа от сервера
            memset(buffer, 0, BUFFER_SIZE);
            const int bytesRead = recv(clientSocket, buffer, BUFFER_SIZE - 1, 0);


            if (bytesRead > 0) {
                std::cout << "Ответ сервера:{" << std::endl;
                nlohmann::json response = nlohmann::json::parse(buffer);
                std::cout << "\t\"status\": " << response["status"] << std::endl;
                std::cout << "\t\"message\": " << response["message"] << std::endl;
                if (response["status"] == "success" && cmd != "INSERT") {
                    std::cout << "\t\"data\": " << response["data"].dump(15) << std::endl;
                    std::cout << "\t\"count : " << response["count"] << std::endl;
                }


            } else if (bytesRead == 0) {
                std::cout << "Сервер отключился" << std::endl;
                break;
            } else {
                std::cerr << "Ошибка получения данных" << std::endl;
                break;
            }
        }

        // Закрытие сокета
        close(clientSocket);
        std::cout << "Клиент завершил работу" << std::endl;
        return 0;
    }catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
        std::cout << "ошибка" << std::endl;
    }
}


/*INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}
INSERT ab {"name": "Anna", "age": 25}*/