//
// Created by dimasik on 20.11.2025.
//

#ifndef INC_1PRAKA_MYVECTOR_H
#define INC_1PRAKA_MYVECTOR_H

#include <stdexcept>


template<typename T>
class MyVector {
private:
    T* data;
    size_t capacity;
    size_t sizeV;

    void resizeV() {
        size_t newCapacity = capacity;
        if (newCapacity == 0) newCapacity = 1;
        else newCapacity = capacity * 2;

        T* newData = new T[newCapacity];

        for (size_t i = 0; i < sizeV; i++) {
            newData[i] = std::move(data[i]);
        }

        delete[] data;
        data = newData;
        capacity = newCapacity;
    };
public:
    MyVector(): data(nullptr), capacity(0), sizeV(0){}

    ~MyVector() {delete[] data;}

    void push_backV(const T& value) {
        if (sizeV >= capacity) resizeV();
        data[sizeV++] = value;
    }
    void push_backV(T&& value) {
        if (sizeV >= capacity) resizeV();
        data[sizeV++] = std::move(value);
    }

    size_t size() const { return sizeV;}

    T& operator[](size_t index) {return data[index];}
    const T& operator[](size_t index) const {return data[index];}


    T* begin() { return data; }
    T* end() { return data + sizeV; }
    const T* begin() const { return data; }
    const T* end() const { return data + sizeV; }
};


#endif //INC_1PRAKA_MYVECTOR_H