#include "includes/doubly-linked-list.h"

DoublyLinkedList::Node::Node(DataType data)//Node Setup
{
    value = data;
    next = nullptr;
    prev = nullptr;
}


DoublyLinkedList::DoublyLinkedList()//DLL constructor
{
    head_ = nullptr;
    tail_ = nullptr;
    size_ = 0;
}


DoublyLinkedList::~DoublyLinkedList()//DLL Destructor
{
    Node *loc = head_;

    while(loc != nullptr)//Goes through entire list and deletes nodes
    {
        Node *next = loc->next;
        delete loc;
        loc = next;
    }

    head_ = nullptr;
    tail_ = nullptr;
    size_ = 0;
}


unsigned int DoublyLinkedList::size() const
{
    return size_;//return current size
}


unsigned int DoublyLinkedList::capacity() const
{
    return CAPACITY;//return max capacity from the header
}


bool DoublyLinkedList::empty() const
{
    return size_ == 0;//empty if size is 0
}


bool DoublyLinkedList::full() const
{
    return size_ == CAPACITY;//DLL is full if size is at capacity
}

DoublyLinkedList::DataType DoublyLinkedList::select(unsigned int index) const
{
    if (!head_)//if no head then nothing to select
    {
        return NULL;//switch to return Datatype();
    }

    if(index >= size_)//if outside range return last entries val
    {
        index = size_ - 1;
    }

    Node *loc = head_;//sets iterator to the head of the list

    for(unsigned int i = 0; i < index; i++)
    {
        if(!loc->next)//if loop hits end of list it returns last val
        {
            return tail_->value;
        }
        loc = loc->next;
    }

    return loc->value;//returns value once it reaches
}

unsigned int DoublyLinkedList::search(DataType value) const
{
    unsigned int i = 0;
    Node *loc = head_;

    while(loc)//while value is not null continue this loop
    {
        if(loc->value == value)//if value of that node is equal to the value return the nodes index
        {
            return i;
        }
        loc = loc->next;
        i++;
    }

    return size_;//return size if it can't find it
}


void DoublyLinkedList::print() const
{
    Node *loc = head_;//set location as head and then iterate through the entire DLL till u hit a nullptr (end)

    while(loc != nullptr)
    {
        std::cout << loc->value << " ";
        loc = loc->next;
    }
}

DoublyLinkedList::Node* DoublyLinkedList::getNode(unsigned int index) const
{
    if(index >= size_)//return nullptr if outside of range
    {
        return nullptr;
    }

    Node *loc = head_;

    for(unsigned int i=0; i<index; i++)//go through DLL till u reach location
    {
        loc = loc->next;
    }

    return loc;
}

bool DoublyLinkedList::insert(DataType value, unsigned int index)
{
    if(index > size_||full())//if out of range or if the thing is full its false
    {
        return false;
    }

    Node *newNode = new Node(value);

    if(index == 0)//if index is 0 then make it the head
    {
        newNode->next = head_;

        if(head_ != nullptr)
        {
            head_->prev = newNode;
        }
        else
        {
            tail_ = newNode;
        }

        head_ = newNode;
    }
    else if(index == size_)//if index is the size then its the tail
    {
        newNode->prev = tail_;

        if(tail_ != nullptr)
        {
            tail_->next = newNode;
        }
        else
        {
            head_ = newNode;
        }

        tail_ = newNode;
    }
    else//otherwise insert normally by putting pointers both ways for the previous and afters nodes
    {
        Node *loc = getNode(index);
        newNode->prev = loc->prev;
        newNode->next = loc;

        if(loc->prev != nullptr)
        {
            loc->prev->next = newNode;
        }

        loc->prev = newNode;

    }

    size_ ++;//increase size and return true
    return true;
}

bool DoublyLinkedList::insert_front(DataType value)
{
    return insert(value, 0);//insert value at the front
}


bool DoublyLinkedList::insert_back(DataType value)
{
    return insert(value, size_);//insert at the back
}


bool DoublyLinkedList::remove(unsigned int index)
{
    if(index>=size_||empty())//can't remove from empty or out of range
    {
        return false;
    }


    if(size_ == 1 && index == 0)//Special case for deleting head
    {
        delete head_;
        head_ = nullptr;
        tail_ = nullptr;
        size_ = 0;
        return true;
    }

    Node *loc = getNode(index);

    if(loc == head_)//if location is head set head as next node
    {
        head_ = loc->next;
    }
    else
    {//if location is not head go to previous node and link it to the node after the removed one
        loc->prev->next = loc->next;
    }
    if(loc == tail_)//if its the tail being removed then set the tail as the previous node
    {
        tail_ = loc->prev;
    }
    else//otherwise set the next nodes previous node as the previous node of the deleted
    {
        loc->next->prev = loc->prev;
    }

    delete loc;//delete the node at index
    loc = nullptr;
    size_--;//reduce size of dll

    return true;
}


bool DoublyLinkedList::remove_front()
{
    return remove(0);
}


bool DoublyLinkedList::remove_back()
{
    return remove(size_-1);
}


bool DoublyLinkedList::replace(unsigned int index, DataType value)
{
    if(index>=size_||empty())//if index is out of range or the dll is empty can't replace anything
    {
        return false;
    }

    Node *loc = getNode(index);//otherwise get the node at the location
    loc->value = value;//replace the value of the node with the new value

    return true;
}
