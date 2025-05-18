#include "df/dataframe.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <numeric>
#include <stdexcept>
#include <type_traits>

DataFrame::DataFrame() : rowCount(0) {}

DataFrame::DataFrame(const std::map<std::string, ColumnData>& data) : columns(data) {
    if (!columns.empty()) {
        rowCount = std::visit([](auto&& vec) { return vec.size(); }, columns.begin()->second);
        for (const auto& [colName, colData] : columns) {
            size_t colSize = std::visit([](auto&& vec) { return vec.size(); }, colData);
            if (colSize != rowCount) {
                throw std::invalid_argument("All columns must have the same number of rows.");
            }
        }
    } else {
        rowCount = 0;
    }
}

DataFrame::~DataFrame() {}

void DataFrame::addColumn(const std::string& columnName, const ColumnData& data) {
    size_t newRowCount = std::visit([](auto&& vec) { return vec.size(); }, data);
    if (columns.empty()) {
        rowCount = newRowCount;
    } else if (newRowCount != rowCount) {
        throw std::invalid_argument("All columns must have the same number of rows.");
    }
    columns[columnName] = data;
}

void DataFrame::removeColumn(const std::string& columnName) {
    columns.erase(columnName);
}

bool DataFrame::columnExists(const std::string& columnName) const {
    return columns.find(columnName) != columns.end();
}

DataFrame::ColumnData& DataFrame::operator[](const std::string& columnName) {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    return columns[columnName];
}

size_t DataFrame::numRows() const {
    return rowCount;
}

size_t DataFrame::numColumns() const {
    return columns.size();
}

DataFrame DataFrame::operator()(size_t startRow, size_t endRow) const {
    if (startRow >= endRow || endRow > rowCount) {
        throw std::out_of_range("Invalid row indices.");
    }
    DataFrame slicedDF;
    for (const auto& [colName, colData] : columns) {
        ColumnData slicedData = std::visit(
            [startRow, endRow](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                return VecType(vec.begin() + startRow, vec.begin() + endRow);
            },
            colData);
        slicedDF.addColumn(colName, slicedData);
    }
    return slicedDF;
}

DataFrame DataFrame::head(size_t n) const {
    return this->operator()(0, std::min(n, rowCount));
}

DataFrame DataFrame::tail(size_t n) const {
    size_t start = rowCount > n ? rowCount - n : 0;
    return this->operator()(start, rowCount);
}

DataFrame DataFrame::select(const std::vector<std::string>& columnNames) const {
    DataFrame selectedDF;
    for (const auto& colName : columnNames) {
        if (columnExists(colName)) {
            selectedDF.addColumn(colName, columns.at(colName));
        } else {
            throw std::out_of_range("Column does not exist: " + colName);
        }
    }
    return selectedDF;
}

DataFrame DataFrame::filter(const std::function<bool(const std::map<std::string, ColumnData>&, size_t)>& condition) const {
    std::vector<size_t> selectedIndices;
    for (size_t i = 0; i < rowCount; ++i) {
        if (condition(columns, i)) {
            selectedIndices.push_back(i);
        }
    }
    DataFrame filteredDF;
    for (const auto& [colName, colData] : columns) {
        ColumnData filteredData = std::visit(
            [&selectedIndices](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                VecType newVec;
                for (size_t idx : selectedIndices) {
                    newVec.push_back(vec[idx]);
                }
                return newVec;
            },
            colData);
        filteredDF.addColumn(colName, filteredData);
    }
    return filteredDF;
}

void DataFrame::sort(const std::string& columnName, bool ascending) {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    std::vector<size_t> indices(rowCount);
    std::iota(indices.begin(), indices.end(), 0);

    auto& colData = columns[columnName];
    std::visit(
        [&](auto&& vec) {
            using ValueType = typename std::decay_t<decltype(vec)>::value_type;
            std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
                if (ascending)
                    return vec[i1] < vec[i2];
                else
                    return vec[i1] > vec[i2];
            });
        },
        colData);

    for (auto& [_, colData] : columns) {
        std::visit(
            [&](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                VecType sortedVec(rowCount);
                for (size_t i = 0; i < rowCount; ++i) {
                    sortedVec[i] = vec[indices[i]];
                }
                vec = sortedVec;
            },
            colData);
    }
}

void DataFrame::fillna(const ColumnData& value) {
    for (auto& [colName, colData] : columns) {
        std::visit(
            [&](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                using ValueType = typename VecType::value_type;
                if (std::holds_alternative<VecType>(value)) {
                    ValueType fillValue = std::get<VecType>(value)[0];
                    for (auto& elem : vec) {
                        if constexpr (std::is_same_v<ValueType, double>) {
                            if (std::isnan(elem)) {
                                elem = fillValue;
                            }
                        } else if constexpr (std::is_same_v<ValueType, int>) {
                            if (elem == 0) {
                                elem = fillValue;
                            }
                        } else if constexpr (std::is_same_v<ValueType, std::string>) {
                            if (elem.empty()) {
                                elem = fillValue;
                            }
                        } else if constexpr (std::is_same_v<ValueType, bool>) {
                            elem = fillValue;
                        }
                    }
                }
            },
            colData);
    }
}

double DataFrame::mean(const std::string& columnName) const {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    const auto& colData = columns.at(columnName);
    return std::visit(
        [&](auto&& vec) -> double {
            using VecType = std::decay_t<decltype(vec)>;
            using ValueType = typename VecType::value_type;
            if constexpr (std::is_arithmetic_v<ValueType>) {
                double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
                return sum / vec.size();
            } else {
                throw std::invalid_argument("Mean is not defined for non-numeric data.");
            }
        },
        colData);
}

double DataFrame::sum(const std::string& columnName) const {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    const auto& colData = columns.at(columnName);
    return std::visit(
        [&](auto&& vec) -> double {
            using VecType = std::decay_t<decltype(vec)>;
            using ValueType = typename VecType::value_type;
            if constexpr (std::is_arithmetic_v<ValueType>) {
                return std::accumulate(vec.begin(), vec.end(), 0.0);
            } else {
                throw std::invalid_argument("Sum is not defined for non-numeric data.");
            }
        },
        colData);
}

double DataFrame::max(const std::string& columnName) const {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    const auto& colData = columns.at(columnName);
    return std::visit(
        [&](auto&& vec) -> double {
            using VecType = std::decay_t<decltype(vec)>;
            using ValueType = typename VecType::value_type;
            if constexpr (std::is_arithmetic_v<ValueType>) {
                return *std::max_element(vec.begin(), vec.end());
            } else {
                throw std::invalid_argument("Max is not defined for non-numeric data.");
            }
        },
        colData);
}

double DataFrame::min(const std::string& columnName) const {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    const auto& colData = columns.at(columnName);
    return std::visit(
        [&](auto&& vec) -> double {
            using VecType = std::decay_t<decltype(vec)>;
            using ValueType = typename VecType::value_type;
            if constexpr (std::is_arithmetic_v<ValueType>) {
                return *std::min_element(vec.begin(), vec.end());
            } else {
                throw std::invalid_argument("Min is not defined for non-numeric data.");
            }
        },
        colData);
}

void DataFrame::add(const std::string& columnName, double value) {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    auto& colData = columns[columnName];
    std::visit(
        [value](auto&& vec) {
            using VecType = std::decay_t<decltype(vec)>;
            using ValueType = typename VecType::value_type;
            if constexpr (std::is_arithmetic_v<ValueType>) {
                for (auto& elem : vec) {
                    elem += static_cast<ValueType>(value);
                }
            } else {
                throw std::invalid_argument("Addition is not supported for non-numeric data.");
            }
        },
        colData);
}

void DataFrame::subtract(const std::string& columnName, double value) {
    add(columnName, -value);
}

void DataFrame::multiply(const std::string& columnName, double value) {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    auto& colData = columns[columnName];
    std::visit(
        [value](auto&& vec) {
            using VecType = std::decay_t<decltype(vec)>;
            using ValueType = typename VecType::value_type;
            if constexpr (std::is_arithmetic_v<ValueType>) {
                for (auto& elem : vec) {
                    elem *= static_cast<ValueType>(value);
                }
            } else {
                throw std::invalid_argument("Multiplication is not supported for non-numeric data.");
            }
        },
        colData);
}

void DataFrame::divide(const std::string& columnName, double value) {
    if (value == 0) {
        throw std::invalid_argument("Division by zero.");
    }
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    auto& colData = columns[columnName];
    std::visit(
        [value](auto&& vec) {
            using VecType = std::decay_t<decltype(vec)>;
            using ValueType = typename VecType::value_type;
            if constexpr (std::is_arithmetic_v<ValueType>) {
                for (auto& elem : vec) {
                    elem /= static_cast<ValueType>(value);
                }
            } else {
                throw std::invalid_argument("Division is not supported for non-numeric data.");
            }
        },
        colData);
}

void DataFrame::display(size_t n) const {
    size_t displayRows = std::min(n, rowCount);
    for (const auto& [colName, _] : columns) {
        std::cout << colName << "\t";
    }
    std::cout << "\n";
    for (size_t i = 0; i < displayRows; ++i) {
        for (const auto& [_, colData] : columns) {
            std::visit(
                [i](auto&& vec) {
                    std::cout << vec[i] << "\t";
                },
                colData);
        }
        std::cout << "\n";
    }
}

DataFrame DataFrame::read_csv(const std::string& filename) {
    DataFrame df;
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filename);
    }
    std::string line;
    if (!std::getline(file, line)) {
        throw std::runtime_error("File is empty.");
    }
    std::istringstream headerStream(line);
    std::vector<std::string> columnNames;
    std::string columnName;
    while (std::getline(headerStream, columnName, ',')) {
        columnNames.push_back(columnName);
        df.columns[columnName] = std::vector<std::string>();
    }
    while (std::getline(file, line)) {
        std::istringstream lineStream(line);
        std::string cell;
        size_t colIndex = 0;
        while (std::getline(lineStream, cell, ',')) {
            if (colIndex >= columnNames.size()) {
                throw std::runtime_error("Row has more columns than header.");
            }
            std::string& colName = columnNames[colIndex];
            auto& colData = df.columns[colName];
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
        if (colIndex != columnNames.size()) {
            throw std::runtime_error("Row has fewer columns than header.");
        }
        df.rowCount++;
    }
    return df;
}

void DataFrame::to_csv(const std::string& filename) const {
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filename);
    }
    for (auto it = columns.begin(); it != columns.end(); ++it) {
        file << it->first;
        if (std::next(it) != columns.end()) {
            file << ",";
        }
    }
    file << "\n";
    for (size_t i = 0; i < rowCount; ++i) {
        size_t colIndex = 0;
        for (const auto& [_, colData] : columns) {
            std::visit(
                [&](auto&& vec) {
                    file << vec[i];
                },
                colData);
            if (colIndex < columns.size() - 1) {
                file << ",";
            }
            colIndex++;
        }
        file << "\n";
    }
}
