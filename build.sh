#!/bin/bash

mkdir -p bin/static

g++ -std=c++17 -Iinclude -c main.cpp -o bin/main.o

g++ -std=c++17 -Iinclude -c src/df/dataframe.cpp -o bin/static/dataframe.o
g++ -std=c++17 -Iinclude -c src/df/math.cpp -o bin/static/math.o
g++ -std=c++17 -Iinclude -c src/df/stats.cpp -o bin/static/stats.o
g++ -std=c++17 -Iinclude -c src/df/io.cpp -o bin/static/io.o
g++ -std=c++17 -Iinclude -c src/df/index.cpp -o bin/static/index.o

ar rcs bin/static/dataframe_lib.a bin/static/dataframe.o bin/static/math.o bin/static/stats.o bin/static/io.o bin/static/index.o

g++ bin/main.o -Lbin/static -l:dataframe_lib.a -o bin/dataframe_demo

echo "Compilation complete. Run ./bin/dataframe_demo to execute the program." 