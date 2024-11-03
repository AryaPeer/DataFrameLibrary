#!/bin/bash

g++ -c src/main.cpp -o bin/main.o

g++ -c src/dfds/binary-search-tree.cpp -o bin/static/binary-search-tree.o
g++ -c src/dfds/avl-tree.cpp -o bin/static/avl-tree.o
g++ -c src/dfds/circular-queue.cpp -o bin/static/circular-queue.o
g++ -c src/dfds/doubly-linked-list.cpp -o bin/static/doubly-linked-list.o
g++ -c src/dfds/dynamic-stack.cpp -o bin/static/dynamic-stack.o
g++ -c src/dfds/sequential-list.cpp -o bin/static/sequential-list.o
g++ -c src/dfds/dataframe.cpp -o bin/static/dataframe.o

ar rcs bin/static/libdfds.a bin/static/binary-search-tree.o bin/static/avl-tree.o bin/static/circular-queue.o bin/static/doubly-linked-list.o bin/static/dynamic-stack.o bin/static/sequential-list.o bin/static/dataframe.o

g++ bin/main.o -Lbin/static -l:libdfds.a -o bin/execute_program

# ./bin/execute_program - Run this to execute the program