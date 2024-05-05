#include "dfds/doubly-linked-list.h"

int main() {
    // Create a DoublyLinkedList object
    DoublyLinkedList list;

    // Insert some elements into the list
    list.insert_back(1);
    list.insert_back(2);
    list.insert_back(3);

    // Print the elements in the list
    list.print();

    return 0;
}
