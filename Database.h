#ifndef PROVERKA_DATABASE_H
#define PROVERKA_DATABASE_H
#include <fstream>
#include <random>
#include <filesystem>

#include "hashMap.h"

static std::mt19937 gen(std::chrono::steady_clock::now().time_since_epoch().count());
static std::uniform_int_distribution<uint32_t> dist(0, 1025);

class Database {
private:
    static std::string generateId();
    static bool matchesCondition(const nlohmann::json& doc, const std::string& field, const nlohmann::json& condition);
    static bool matchesQuery(const nlohmann::json& doc, const nlohmann::json& query);
public:
    static void insertDoc(HashMap* map, const std::string& jsonCommand);

    static void findDoc(const HashMap* map, const std::string& jsonCommand);

    static void deleteDoc(HashMap* map, const std::string& jsonCommand);

};


#endif //PROVERKA_DATABASE_H