#include "hashMap.h"

#include <fstream>

#include "simlyList.h"
#include <iostream>

using namespace std;
using namespace nlohmann;

HashMap::HashMapNode::HashMapNode() : list(nullptr){}

HashMap::HashMap(const size_t& cap): capacity(cap), size(0) {
    table = new HashMapNode[capacity];
    for (size_t i = 0; i < capacity; i++) {
        table[i].list = new SimplyList();
    }
}

HashMap::~HashMap() {
    for (size_t i = 0; i < capacity; i++) {
        delete table[i].list;
    }
    delete[] table;
}

size_t HashMap::getCapacity() const {
    return capacity;
}

int HashMap::hashFunction(const std::string &str) const {
    unsigned long hash = base;

    for (const auto& c : str) {
        hash ^= static_cast<unsigned char>(c);
        hash *= prime;
    }
    return hash % capacity;
}

void HashMap::hashMapInsert(const std::string &key,const json &value) {
    if (static_cast<double>(size) / capacity >= 0.75) {
        rehash();
    }
    const int index = hashFunction(key);
    table[index].list->addHead(key, value);
    size++;
}

bool HashMap::deleteById(const std::string &id) {
    if (table == nullptr || id.empty()) return false;

    const int index = hashFunction(id);

    if (table[index].list == nullptr) {
        return false;
    }
    if (table[index].list->deleteByKey(id)) {
        size--;
        return true;
    }
    return false;
}



MyVector<pair<string,json>> HashMap::items() const {
    MyVector<std::pair<std::string, json>> result;
    for (size_t i = 0; i < capacity; i++) {
        for (const auto& item : table[i].list->items()) {
            result.push_backV(item);
        }
    }
    return result;
}


void HashMap::print() const {
    if (table == nullptr) return;
    cout << "Размер: " << size << "/" << capacity << endl;
    for (size_t i = 0; i < capacity; i++) {
        if (table[i].list == nullptr) {
            cout << "[" << i << " [NULL]" << endl;
        } else {
            cout << "[" << i << "] ";
            table[i].list->printList();
        }
    }
}

void HashMap::saveToFile(const string& filename) const {
    json data = json::array();
    auto allItems = items();
    for (const auto& item : allItems) {
        data.push_back(item.second);
    }

    ofstream file(filename);
    file << data.dump(4);
}

void HashMap::rehash() {
    const size_t oldCapacity = capacity;
    HashMapNode* oldTable = table;

    capacity = capacity * 2 + 1;
    size = 0;

    table = new HashMapNode[capacity];
    for (size_t i = 0; i < capacity; i++) {
        table[i].list = new SimplyList();
    }

    for (size_t i = 0; i < oldCapacity; i++) {
        const SimplyList* currentList = oldTable[i].list;
        auto current = currentList->getHead();

        while (current != nullptr) {
            int newIndex = hashFunction(current->id_);
            table[newIndex].list->addHead(current->id_, current->data);

            size++;
            current = current->next;
        }
    }

    for (size_t i = 0; i < oldCapacity; i++) {
        delete oldTable[i].list;
    }

    delete[] oldTable;

}

std::pair<std::string, std::string> HashMap::searchByKey(const std::string &key) const {
    if (table == nullptr) return {"", ""};

    size_t index = hashFunction(key);
    if (table[index].list == nullptr) return {"", ""};

    return table[index].list->searchByKey(key);
}


