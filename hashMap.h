#ifndef HASHMAP_H
#define HASHMAP_H

#include <string>
#include "simlyList.h"
#include "myVector.h"


const unsigned long base = 2166136261;
const unsigned long prime = 16777619;

class HashMap {
private:
    struct HashMapNode {
        SimplyList* list;

        HashMapNode();
    };
    HashMapNode* table;
    size_t capacity;
    size_t size;
public:
    explicit HashMap(const size_t& cap);
    ~HashMap();

    [[nodiscard]] size_t getCapacity() const;

    [[nodiscard]] int hashFunction(const std::string& str) const;
    void hashMapInsert(const std::string& key,const nlohmann::json& value);
    bool deleteById(const std::string& id);

    [[nodiscard]]MyVector<std::pair<std::string, nlohmann::json>> items() const;

    void saveToFile(const std::string& filename) const;
    void loadFromFile(const std::string& filename);
    void print() const;
    void rehash();
    std::pair<std::string, std::string> searchByKey(const std::string& key) const;

};


#endif