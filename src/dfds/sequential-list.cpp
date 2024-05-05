#include "sequential-list.h"

SequentialList::SequentialList(unsigned int cap)
{
    data_ = new DataType[cap];
    capacity_ = cap;
    size_ = 0;
}

SequentialList::~SequentialList()
{
    delete[] data_;
    data_ = nullptr;
}

unsigned int SequentialList::size() const
{
    return size_;
}

unsigned int SequentialList::capacity() const
{
    return capacity_;
}

bool SequentialList::empty() const
{
    return size_ == 0;
}

bool SequentialList::full() const
{
    return size_ == capacity_;
}

SequentialList::DataType SequentialList::select(unsigned int index) const
{
    if (index >= size_)
    {
        return data_[size_ - 1];
    }
    else
    {
        return data_[index];
    }
}

unsigned int SequentialList::search(DataType val) const
{
    for (unsigned int i = 0; i < size_; i++)
    {
        if (data_[i] == val)
        {
            return i;
        }
    }
    return size_;
}

void SequentialList::print() const
{
    for (int i = 0; i < size_; i++)
    {
        std::cout << data_[i] << " ";
    }
}


bool SequentialList::insert(DataType val, unsigned int index)
{
    if (index > size_ || full())
    {
        return false;
    }
    else
    {
        for (unsigned int i = size_; i > index; i--)
        {
            data_[i] = data_[i - 1];
        }
        data_[index] = val;
        size_++;
        return true;
    }
}


bool SequentialList::insert_front(DataType val)
{
    return insert(val, 0);
}


bool SequentialList::insert_back(DataType val)
{
    return insert(val, size_);
}


bool SequentialList::remove(unsigned int index)
{
    if (index >= size_ || empty())
    {
        return false;
    }
    else
    {
        for (unsigned int i = index; i < size_ - 1; i++)
        {
            data_[i] = data_[i + 1];
        }
        data_[size_ - 1] = 0;
        size_--;
        return true;
    }
}


bool SequentialList::remove_front()
{
    return remove(0);
}


bool SequentialList::remove_back()
{
    return remove(size_ - 1);
}


bool SequentialList::replace(unsigned int index, DataType val)
{
    if (index >= size_)
    {
        return false;
    }
    else
    {
        data_[index] = val;
        return true;
    }
}
