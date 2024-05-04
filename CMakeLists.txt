cmake_minimum_required(VERSION 3.0.2)
project(df_ds_library)

# Enable C++11 support
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add library headers
include_directories(headers)

# Add library source files
set(SOURCES
    
)

# Create the library
add_library(df_ds_library ${SOURCES})
