#include <stdexcept>
#include <utility>
#include <iostream>

#include "simlyList.h"

using namespace std;
using namespace nlohmann;

SimplyList::SimplyNode::SimplyNode(const string& id,const json& value) :
                                    id_(std::move(id))
                                    ,data(std::move(value))
                                    ,next(nullptr){}

SimplyList::SimplyList() : head(nullptr), tail(nullptr) {}

SimplyList::~SimplyList() {
    while (head != nullptr) {
        SimplyNode* temp = head;
        head = head->next;
        delete temp;
    }
}

MyVector<pair<string,json>> SimplyList::items() const {
    MyVector<pair<string, json>> result;
    for (SimplyNode* curr = head; curr != nullptr; curr = curr->next) {
        result.push_backV(make_pair(curr->id_, curr->data));
    }
    return result;
}

void SimplyList::addHead(const string &key, const json &value) {
    if (value.empty()) throw runtime_error("Значение пустое");
    if (key.empty()) throw runtime_error("Id пустой");
    const auto newNode = new SimplyNode(key, value);
    newNode->next = head;
    head = newNode;
    if (!tail) tail = newNode;
}

bool SimplyList::deleteByKey(const string &key) {
    if (head == nullptr) return false;

    if (head->id_ == key) {
        const SimplyNode* temp = head;
        head = head->next;
        if (tail == temp) tail = nullptr;
        delete temp;
        return true;
    }

    SimplyNode* current = head;
    while (current->next != nullptr && current->next->id_ != key) {
        current = current->next;
    }

    if (current->next == nullptr) return false;

    const SimplyNode* temp = current->next;
    current->next = temp->next;

    if (temp == tail) tail = current;

    delete temp;
    return true;
}


void SimplyList::printList() const {
    auto current = head;
    int index = 0;
    while (current != nullptr) {
        cout << "[" << current->id_ << " : " << current->data << " ]";
        cout << " -> ";
        current = current->next;
        index++;
    }
    cout << "[NULL]" << endl;
}

pair<string, string> SimplyList::searchByKey(const std::string &key) const {
    SimplyNode* current = head;
    while (current != nullptr) {
        if (current->id_ == key) {
            return make_pair(current->id_, current->data.dump(4));
        }
        current = current ->next;
    }
    return make_pair("", "");
}


