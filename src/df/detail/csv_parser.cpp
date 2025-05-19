#include "df/detail/csv_parser.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace df {
namespace detail {

CsvParseResult parse_csv(const std::string& filename) {
    CsvParseResult result;
    result.rowCount = 0;
    
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filename);
    }

    std::string line;
    // Parse header
    if (!std::getline(file, line)) {
        throw std::runtime_error("File is empty.");
    }
    
    std::istringstream headerStream(line);
    std::string columnName;
    while (std::getline(headerStream, columnName, ',')) {
        result.columnNames.push_back(columnName);
        result.data[columnName] = std::vector<std::string>();
    }

    // Parse data rows
    while (std::getline(file, line)) {
        std::istringstream lineStream(line);
        std::string cell;
        size_t colIndex = 0;
        
        while (std::getline(lineStream, cell, ',')) {
            if (colIndex >= result.columnNames.size()) {
                throw std::runtime_error("Row has more columns than header.");
            }
            
            std::string& colName = result.columnNames[colIndex];
            auto& colData = result.data[colName];
            
            // Currently we're storing everything as strings
            // TODO: Add type inference for columns
            std::visit(
                [&](auto&& vec) {
                    using VecType = std::decay_t<decltype(vec)>;
                    if constexpr (std::is_same_v<VecType, std::vector<std::string>>) {
                        vec.push_back(cell);
                    }
                },
                colData);
            
            colIndex++;
        }
        
        if (colIndex != result.columnNames.size()) {
            throw std::runtime_error("Row has fewer columns than header.");
        }
        
        result.rowCount++;
    }
    
    return result;
}

void write_csv(const std::string& filename, 
               const std::vector<std::string>& columnNames, 
               const std::map<std::string, std::variant<std::vector<int>, std::vector<double>, std::vector<std::string>, std::vector<bool>>>& data,
               size_t rowCount) {
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filename);
    }

    // Write header
    for (size_t i = 0; i < columnNames.size(); i++) {
        file << columnNames[i];
        if (i < columnNames.size() - 1) {
            file << ",";
        }
    }
    file << "\n";

    // Write data rows
    for (size_t i = 0; i < rowCount; ++i) {
        for (size_t colIndex = 0; colIndex < columnNames.size(); ++colIndex) {
            const auto& colName = columnNames[colIndex];
            const auto& colData = data.at(colName);
            
            std::visit(
                [&](auto&& vec) {
                    file << vec[i];
                },
                colData);
                
            if (colIndex < columnNames.size() - 1) {
                file << ",";
            }
        }
        file << "\n";
    }
}

} // namespace detail
} // namespace df