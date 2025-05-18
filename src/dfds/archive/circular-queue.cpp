#include "includes/circular-queue.h"
#include <iostream>

const CircularQueue::QueueItem CircularQueue::EMPTY_QUEUE = -999;

CircularQueue::CircularQueue() {
    capacity_ = 16;
    items_ = new QueueItem[capacity_];
    head_=0;
    tail_=0;
    size_=0;
}

CircularQueue::CircularQueue(unsigned int capacity) {
    capacity_ = capacity;
    items_ = new QueueItem[capacity_];
    head_=0;
    tail_=0;
    size_=0;
}

CircularQueue::~CircularQueue() {
    delete[] items_;
    items_ = nullptr;
}

unsigned int CircularQueue::size() const {
    return size_;
}

bool CircularQueue::empty() const {
    return size_ == 0;
}

bool CircularQueue::full() const {
    return size_==capacity_;
}

CircularQueue::QueueItem CircularQueue::peek() const {
    if(empty()){
        return EMPTY_QUEUE;
    }

    return items_[head_];
}

bool CircularQueue::enqueue(QueueItem value) {
    if(full()){
        return false;
    }

    items_[tail_] = value;
    tail_ = (tail_ + 1) % capacity_;
    size_++;

    return true;
}

CircularQueue::QueueItem CircularQueue::dequeue() {
    if(empty()){
        return EMPTY_QUEUE;
    }

    QueueItem val = items_[head_];
    head_ = (head_+1)%capacity_;
    size_--;

    return val;
}

void CircularQueue::print() const {
    if(empty()){
        std::cout << "Queue is empty";
    }else{
        int index = head_;
        do {
            std::cout << items_[index] << " ";
            index = (index + 1) % capacity_;
        } while (index != tail_);
    }
}
