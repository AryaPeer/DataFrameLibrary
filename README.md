# DF_DS_LIB

A comprehensive C++ library featuring a Pandas-like DataFrame implementation along with efficient data structures, providing template-based implementations for type flexibility and thread-safe operations.

## Features

### Data Structures
- Trees: Binary Search Tree and AVL Tree implementations
- Lists: Doubly Linked List and Sequential List
- Stack: Dynamic stack with auto-resizing
- Queue: Circular queue implementation

### DataFrame Implementation
- DataFrame: Feature-rich Pandas-like implementation with statistical operations
- CSV Support: Read and write operations for data I/O
- Column Operations: Filtering, sorting, and arithmetic operations
- Statistics: Mean, sum, max, min calculations
- Missing Data: Handling of NA values

## Dependencies

- C++11 or later
- Standard Template Library (STL)

## Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/AryaPeer/DF_DS_LIB.git
   cd DF_DS_LIB
   ```

2. **Build the Library**
   ```bash
   bash run.sh
   ```

## Usage

1. **Navigate to main.cpp under src**

2. **Edit File To Include Required Headers**
   ```cpp
   #include "dfds/includes/dataframe.h"
   #include "dfds/includes/binary-search-tree.h"
   // ... other headers as needed
   ```

3. **Implement Whatever You Want (Basic Example Below)**
   ```cpp
   #include "dfds/includes/dataframe.h"
   
   int main() {
       DataFrame df;
       
       // Add data
       df.addColumn("numbers", std::vector<int>{1, 2, 3, 4, 5});
       df.addColumn("names", std::vector<std::string>{"A", "B", "C", "D", "E"});
       
       // Display first 5 rows
       df.display();
       
       return 0;
   }
   ```

## Project Structure

```
DF_DS_LIB/
├── src/
│   ├── dfds/
│   │   ├── includes/      # Header files
│   │   └── ...           # Implementation files
│   └── main.cpp
├── bin/
│   └── static/         # Compiled objects and libraries
├── run.sh             # Build script
└── README.md
```
