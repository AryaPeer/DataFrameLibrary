# DataFrameLibrary

A modern, lightweight C++ library that brings Pandas-style DataFrame capabilities to C++ developers. Designed for flexibility and performance, `DataFrameLibrary` enables intuitive data manipulation, statistical analysis, and CSV I/O operations using standard C++ templates and STL containers.

## Features

### Core DataFrame Functionality

* **Intuitive Column Operations**: Add, access, sort, and filter columns with ease.
* **CSV Support**: Read and write DataFrames to CSV files.
* **Missing Data Handling**: Detect and manage NA/null entries.
* **Mathematical Operations**: Element-wise arithmetic, column aggregations (sum, mean, min, max).
* **Indexing**: Basic row indexing and label-based lookups.
* **Extensibility**: Modular architecture for adding custom transformations or data types.

## Dependencies

* C++17 or later
* Standard Template Library (STL)

## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/AryaPeer/DataFrameLibrary.git
   cd DataFrameLibrary
   ```

2. **Build the Library**

   ```bash
   bash build.sh
   ```

## Usage

1. **Navigate to `main.cpp`**

2. **Include Required Headers**

   ```cpp
   #include "dataframe.hpp"
   #include "index.hpp"
   #include "io.hpp"
   #include "math.hpp"
   #include "stats.hpp"
   ```

3. **Make the Changes you Want**

## Project Structure

```
DataFrameLibrary/
├── include/
│   └── df/
│       ├── dataframe.hpp       # Main DataFrame class
│       ├── index.hpp           # Indexing support
│       ├── io.hpp              # CSV read/write support
│       ├── math.hpp            # Arithmetic operations
│       ├── stats.hpp           # Statistical functions
│       └── detail/
│           └── csv_parser.hpp  # Internal CSV parser
├── src/
│   └── df/
│       ├── dataframe.cpp
│       ├── index.cpp
│       ├── io.cpp
│       ├── math.cpp
│       ├── stats.cpp
│       └── detail/
│           └── csv_parser.cpp
├── main.cpp                    # Demo
├── build.sh                    # Build script
└── README.md
```
