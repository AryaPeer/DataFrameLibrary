#include "includes/dynamic-stack.h"
#include <iostream>


const DynamicStack::StackItem DynamicStack::EMPTY_STACK = -999;

DynamicStack::DynamicStack() {
    init_capacity_ = 16;
    capacity_=init_capacity_;
    items_ = new StackItem[capacity_];
    size_ = 0;
}

DynamicStack::DynamicStack(unsigned int capacity){
    init_capacity_ = capacity;
    capacity_ = init_capacity_;
    items_ = new StackItem[capacity_];
    size_ = 0;
}

DynamicStack::~DynamicStack() {
    delete[] items_;
    items_ = nullptr;
}

unsigned int DynamicStack::size() const {
    return size_;
}

bool DynamicStack::empty() const {
    return size_ == 0;
}

DynamicStack::StackItem DynamicStack::peek() const {
    if (empty()) {
        return EMPTY_STACK;
    }

    return items_[size_-1];
}

void DynamicStack::push(StackItem value) {
    if (size_ == capacity_) {

        capacity_ *= 2;
        StackItem *newArr = new StackItem[capacity_];

        for (unsigned int i = 0; i < size_; i++) {
            newArr[i] = items_[i];
        }

        delete[] items_;
        items_ = newArr;
    }

    items_[size_] = value;
    size_++;
}

DynamicStack::StackItem DynamicStack::pop() {
    if (empty())
    {
        return EMPTY_STACK;
    }

    StackItem val = items_[size_-1];
    size_--;

    if (size_ <= (capacity_/4) && (capacity_ / 2) >= init_capacity_) {

        capacity_ /= 2;
        StackItem* newArr = new StackItem[capacity_];

        for (unsigned int i = 0; i < size_; i++) {
            newArr[i] = items_[i];
        }

        delete[] items_;
        items_ = newArr;
    }

    return val;
}

void DynamicStack::print() const {
    if(empty()){
        std::cout << "Stack is empty";
    }else{
        for (unsigned int i = size_-1; i >= 0; i--) {
            std::cout << items_[i] << " ";
        }
    }
}
