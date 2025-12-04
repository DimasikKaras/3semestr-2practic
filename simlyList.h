#ifndef SIMLYLIST_H
#define SIMLYLIST_H

#include <string>
#include "json.hpp"

#include "myVector.h"

class SimplyList {
private:
    struct SimplyNode{
        std::string id_;
        nlohmann::json data;
        SimplyNode* next;

        SimplyNode(const std::string&  id, const nlohmann::json&  value);
    };
    SimplyNode* head;
    SimplyNode* tail;
public:
    SimplyList();
    ~SimplyList();

    [[nodiscard]] nlohmann::json getData() const {return head->data;}
    [[nodiscard]] SimplyNode * getHead() const { return head; }
    [[nodiscard]] SimplyNode* getTail() const { return tail; }

    [[nodiscard]] MyVector<std::pair<std::string, nlohmann::json>> items() const;
    void addHead(const std::string &key, const nlohmann::json &value);
    void printList() const;
    bool deleteByKey(const std::string& key);

    [[nodiscard]] std::pair<std::string, std::string> searchByKey(const std::string& key) const;
};
#endif