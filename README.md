# DF_DS_LIB
C++ library that contains an implementation of Pandas DataFrames + an implementation of a few Data Structures.

To test that the library works run:

```bash
bash run.sh
```

Generate an object file of your cpp file by changing main.cpp and main.o in eval.sh:

```bash
g++ -c src/your_file.cpp -o bin/your_file.o
```
and then statically link it by changing main.o in eval.sh too:

```bash
g++ bin/your_file.o -Lbin/static -l:libdfds.a -o bin/statically-linked
```