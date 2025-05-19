#include "df/io.hpp"
#include "df/dataframe.hpp"
#include <stdexcept>
#include <fstream>
#include <sstream>
#include <vector>

namespace df {

namespace detail {
    struct CSVParseResult {
        std::map<std::string, ColumnData> data;
    };

    CSVParseResult parse_csv(const std::string& filename) {
        std::ifstream file(filename);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + filename);
        }

        CSVParseResult result;
        std::string line;
        
        // Read header
        if (std::getline(file, line)) {
            std::vector<std::string> headers;
            std::stringstream ss(line);
            std::string cell;
            
            while (std::getline(ss, cell, ',')) {
                // Remove quotes if present
                if (!cell.empty() && cell.front() == '"' && cell.back() == '"') {
                    cell = cell.substr(1, cell.length() - 2);
                }
                headers.push_back(cell);
            }
            
            // Initialize columns
            for (const auto& header : headers) {
                result.data[header] = DoubleColumn{};
            }
            
            // Read data rows
            while (std::getline(file, line)) {
                std::stringstream ss(line);
                std::string cell;
                size_t col = 0;
                
                while (std::getline(ss, cell, ',') && col < headers.size()) {
                    // Remove quotes if present
                    if (!cell.empty() && cell.front() == '"' && cell.back() == '"') {
                        cell = cell.substr(1, cell.length() - 2);
                    }
                    
                    // Try to convert to number
                    try {
                        double value = std::stod(cell);
                        auto& column = std::get<DoubleColumn>(result.data[headers[col]]);
                        column.push_back(value);
                    } catch (...) {
                        // If can't convert to number, treat as NA
                        auto& column = std::get<DoubleColumn>(result.data[headers[col]]);
                        column.push_back(NA_VALUE);
                    }
                    
                    col++;
                }
                
                // Fill in missing values if row is too short
                while (col < headers.size()) {
                    auto& column = std::get<DoubleColumn>(result.data[headers[col]]);
                    column.push_back(NA_VALUE);
                    col++;
                }
            }
        }
        
        return result;
    }
    
    void write_csv(const std::string& filename, const DataFrame& df) {
        std::ofstream file(filename);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file for writing: " + filename);
        }

        // Write header
        std::vector<std::string> colNames = df.getColumnNames();
        for (size_t i = 0; i < colNames.size(); ++i) {
            file << colNames[i];
            if (i < colNames.size() - 1) {
                file << ",";
            }
        }
        file << "\n";

        // Write data
        for (size_t row = 0; row < df.numRows(); ++row) {
            for (size_t col = 0; col < colNames.size(); ++col) {
                const auto& colName = colNames[col];
                const auto& colData = df[colName];
                
                std::visit([&](const auto& vec) {
                    using VecType = std::decay_t<decltype(vec)>;
                    
                    if (row < vec.size()) {
                        if constexpr (std::is_same_v<VecType, IntColumn>) {
                            if (vec[row].is_na()) {
                                file << "NA";
                            } else {
                                file << vec[row].value_unsafe();
                            }
                        } 
                        else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
                            if (vec[row].is_na()) {
                                file << "NA";
                            } else {
                                file << vec[row].value_unsafe();
                            }
                        }
                        else if constexpr (std::is_same_v<VecType, BoolColumn>) {
                            if (vec[row].is_na()) {
                                file << "NA";
                            } else {
                                file << (vec[row].value_unsafe() ? "true" : "false");
                            }
                        }
                        else if constexpr (std::is_same_v<VecType, StringColumn>) {
                            if (!vec[row].has_value()) {
                                file << "NA";
                            } else {
                                // Quote strings that contain commas
                                std::string val = vec[row].value();
                                if (val.find(',') != std::string::npos) {
                                    file << "\"" << val << "\"";
                                } else {
                                    file << val;
                                }
                            }
                        }
                    } else {
                        file << "NA";
                    }
                }, colData);
                
                if (col < colNames.size() - 1) {
                    file << ",";
                }
            }
            file << "\n";
        }
    }
}

namespace io {

DataFrame read_csv(const std::string& filename) {
    // Use the local implementation
    auto parseResult = detail::parse_csv(filename);
    
    // Convert parse result to DataFrame
    DataFrame df;
    for (const auto& [colName, colData] : parseResult.data) {
        df.addColumn(colName, colData);
    }
    
    return df;
}

void to_csv(const DataFrame& df, const std::string& filename, const CSVWriteOptions& options) {
    // Use the improved CSV writer
    detail::write_csv(filename, df);
}

} // namespace io
} // namespace df