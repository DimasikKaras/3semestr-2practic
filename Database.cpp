#include "Database.h"
#include <iostream>

using namespace std;
using namespace nlohmann;


std::string Database::generateId() {
    const auto now = chrono::duration_cast<chrono::milliseconds>(
    chrono::system_clock::now().time_since_epoch()).count();
    const uint32_t random_num = dist(gen);
    return to_string(now) + "_" + to_string(random_num);
}

bool Database::matchesCondition(const nlohmann::json &doc, const std::string &field, const nlohmann::json &condition) {
    if (!doc.contains(field)) return false;

    const json& value = doc[field];

    if (!condition.is_object()) {
        return value == condition;
    }

    for (auto& [op, cond_val] : condition.items()) {
        if (op == "$eq") {
            if (value != cond_val) return false;
        }
        else if (op == "$gt") {
            if (!(value.is_number() || value.is_string())) return false;
            if (value <= cond_val) return false;
        }
        else if (op == "$lt") {
            if (!(value.is_number() || value.is_string())) return false;
            if (value >= cond_val) return false;
        }
        else if (op == "$in") {
            if (!cond_val.is_array()) return false;
            bool found = false;
            for (const auto& item : cond_val) {
                if (value == item) { found = true; break; }
            }
            if (!found) return false;
        } else if (op == "$like") {
            if (!value.is_string() || !cond_val.is_string()) return false;
            const auto pattern = cond_val.get<string>();
            const auto text = value.get<string>();

            size_t pi = 0, ti = 0;
            const size_t textLen = text.size();
            const size_t patternLen = pattern.size();
            int lastMath = -1, lastStar = -1;

            while (ti < textLen) {
                if (pi < patternLen && (text[ti] == pattern[pi] || pattern[pi] == '_')) {
                    ti++;
                    pi++;
                } else if (pi < patternLen && pattern[pi] == '%') {
                    lastStar = pi++;
                    lastMath = ti;
                } else if (lastStar != -1) {
                    ti = ++lastMath;
                    pi = lastStar + 1;
                } else return false;
            }

            while (pi < patternLen && pattern[pi] == '%') pi++;
            return pi == patternLen;
        }
    }
    return true;
}

bool Database::matchesQuery(const nlohmann::json &doc, const nlohmann::json &query) {
    if (query.contains("$and")) {
        for (const auto& cond : query["$and"]) {
            if (!matchesQuery(doc, cond)) return false;
        }
        return true;
    }


    if (query.contains("$or")) {
        for (const auto& cond : query["$or"]) {
            if (matchesQuery(doc, cond)) return true;
        }
        return false;
    }

    // неявный AND
    for (auto& [field, condition] : query.items()) {
        if (field[0] == '$') continue;
        if (!matchesCondition(doc, field, condition)) {
            return false;
        }
    }
    return true;
}

bool Database::insertDoc(HashMap* map, const std::string& jsonCommand) {
    json doc = json::parse(jsonCommand);
    string id = generateId();
    doc["_id"] = id;
    map->hashMapInsert(id, doc);
    return true;
}

pair<int, json> Database::findDoc(const HashMap *map, const std::string &jsonCommand) {
    json result = json::array();
    const json query = json::parse(jsonCommand);
    const auto allItems = map->items();
    int count = 0;

    for (const auto& [fst, snd] : allItems) {
        if (matchesQuery(snd, query)) {
            auto p = map->searchByKey(fst);
            if (!p.first.empty() && !p.second.empty()) {
                result.push_back(snd);
                count+= 1;
            }
        }
    }
    return {count, result};
}

pair<int, json> Database::deleteDoc(HashMap *map, const std::string &jsonCommand) {
    json result = json::array();
    const json query = json::parse(jsonCommand);
    const auto allItems = map->items();
    int count = 0;

    for (const auto& [fst, snd] : allItems) {
        if (matchesQuery(snd, query)) {
            if (map->deleteById(fst)) {
                result.push_back(snd);
                count+= 1;
            }
        }
    }
    return {count, result};
}




