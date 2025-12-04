#include <fstream>
#include <random>
#include <iostream>
#include <filesystem>

#include "hashMap.h"
#include "Database.h"

using namespace std;
using namespace nlohmann;

void help() {
    cout << "./no_sql_dbms -h, --help    показать эту справку" << endl;
    cout << "./no_sql_dbms <collection> print   вывести хеш-таблицу" << endl;
    cout << "./no_sql_dbms <collection> remove  удалить коллекцию" << endl;
    cout << "./no_sql_dbms <filename> <command> <query> " << endl;
    cout << "=========filename==============" << endl;
    cout
    << "Файл автоматически сохраняется с разрешением .json по пути /директория проекта/my_database/имя вашего файла.json "
            << endl;
    cout << "=========command===============" << endl;
    cout << "Перечень доступных команд: " << endl;
    cout << "insert - добавить документ в коллекцию " << endl;
    cout << "find   - поиск документа по параметрам " << endl;
    cout << "delete - удаление документа по параметрам " << endl;
    cout << "=========query=================" << endl;
    cout << "Доступные операторы и примеры их использования в поиске и в удалении: " << endl;
    cout << R"('{"$eq": [{"_id": "1763888722455_1021"}]}'   [$eq - равенство (используется по умолчанию)])" << endl;
    cout << R"('{"name": "Alice", "city": "London"}'        [неявный AND])" << endl;
    cout << R"('{"$and": [{"age": 25}, {"city": "Paris"}]}' [явный $AND])" << endl;
    cout << R"('{"$or": [{"age": 25}, {"city": "Paris"}]}'  [$or]')" << endl;
    cout << R"('{"age": {"$gt": 20}}'                       [$gt - больше])" << endl;
    cout << R"('{"age": {"$lt": 20}}'                       [$gt - меньше])" << endl;
    cout << R"('{"name": {"$like": "Ali%"}}'                [$like - поиск по маске строки (% - любая строка, _ - один символ)])" << endl;
    cout << R"('{"city": {"$in": ["London", "Paris"]}}'     [$in - проверить принадлежность к массиву])" << endl;
    cout << "=========Примеры использования=========" << endl;
    cout << R"(./no_sql_dbms collection insert '{"name": "Alice", "age": 25, "city": "London"}')" << endl;
    cout << "Вставка документа со случайным сгенерированным id" << endl;
    cout << R"(./no_sql_dbms collection find '{"$or": [{"age": 25}, {"city": "Paris"}]}')" << endl;
    cout << "Поиск документа с оператором $or" << endl;
    cout << R"(./no_sql_dbms collection delete '{"name": {"$like": "A%"}}' )" << endl;
    cout << "Удаление документа с использованием оператора $like" << endl;
}

int main(int argc, char* argv[]) {
    try {
        if (argc == 1 || (argc == 2 && (string(argv[1]) == "-h" || string(argv[1]) == "--help"))) {
            help();
            return 0;

        }
        string collection_name = argv[1];
        string filename = "my_database/" + collection_name + ".json";
        std::filesystem::create_directories("my_database");

        HashMap map(3);

        // Открытие коллекции и загрузка её в hashmap
        json docs = json::array();
        if (ifstream file(filename); file.is_open()) {
            try {
                file >> docs;
            } catch (...) {
                cerr << "Файл повреждён — начинаем с нуля." << endl;
            }
            file.close();
        }

        for (const auto& doc : docs) {
            string id = doc["_id"];
            map.hashMapInsert(id, doc);
        }

        if (argc == 3) {
            if (string(argv[2]).empty()) throw runtime_error("пустой аргумент запроса");
            string command = argv[2];
            if (command == "print") {
                cout << "=== Коллекция: " << collection_name << " ===" << endl;
                map.print();
                return 0;
            } else if (command == "remove") {
                ifstream file(filename);
                if (std::filesystem::exists(filename)) {
                    std::filesystem::remove(filename);
                    cout << "Файл " << collection_name << ".json удалён" << endl;
                    return 0;
                } else {
                    throw runtime_error("файл не найден");
                }
            }else {
                if (command == "find" || command == "insert" || command == "delete") {
                    throw runtime_error("команда '" + command + "' требует json-запрос");
                } else {
                    throw runtime_error("неизвестная команда");
                }
            }
        }

        if (argc != 4) {
            throw runtime_error("ожидается: <коллекция> <команда> <json>");
        }
        if (string(argv[2]).empty()) throw runtime_error("пустой аргумент запроса");
        if (string(argv[3]).empty()) throw runtime_error("пустой аргумент json запроса");
        string command = argv[2];
        string jsonCommand = argv[3];

        if (command == "insert") {
            Database::insertDoc(&map, jsonCommand);
            map.saveToFile(filename);
        } else if (command == "find") {
            Database::findDoc(&map, jsonCommand);
        } else if (command == "delete") {
            Database::deleteDoc(&map, jsonCommand);
            map.saveToFile(filename);
        }else {
            throw runtime_error("неизвестный запрос, введите ключ -h, --help для справки");
        }



    } catch (const exception& e) {
        cout << "Ошибка: " << e.what() << endl;
    }
    return 0;
}

// ./no_sql_dbms collection insert '{"name": "Alice", "age": 25, "city": "London"}'