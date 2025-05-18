# DataFrameLibrary

DataFrame implementation I wrote in C++ to learn more about std::variant, templates, and RAII.

## Build

```bash
bash build.sh
```

Produces `bin/dataframe_demo`.

## Usage

```cpp
#include "df/dataframe.hpp"

df::IntColumn ints = {1, 2, 3, 4, 5};
df::DoubleColumn doubles = {1.1, 2.2, 3.3, 4.4, 5.5};

df::DataFrame frame({
    {"ints", ints},
    {"doubles", doubles},
});

frame.display();
frame.mean("doubles");
frame.add(10).display();
frame.toCSV("out.csv");
```
