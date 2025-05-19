#include "df/dataframe.hpp"
#include "df/index.hpp"
#include "df/groupby.hpp"
#include "df/stats.hpp"
#include "df/math.hpp"
#include "df/io.hpp"
#include <iostream>
#include <algorithm>
#include <numeric>
#include <stdexcept>
#include <type_traits>
#include <random>
#include <cmath>
#include <sstream>
#include <iomanip>

namespace df {

DataFrame::DataFrame() : rowCount(0) {
    index = std::make_shared<Index>(0);
}

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
    
    index = std::make_shared<Index>(rowCount);
}

DataFrame::DataFrame(const DataFrame& other) 
    : columns(other.columns), index(other.index), rowCount(other.rowCount) {}

DataFrame::DataFrame(DataFrame&& other) noexcept 
    : columns(std::move(other.columns)), index(std::move(other.index)), rowCount(other.rowCount) {
    other.rowCount = 0;
}

DataFrame::~DataFrame() {}

DataFrame& DataFrame::operator=(const DataFrame& other) {
    if (this != &other) {
        columns = other.columns;
        index = other.index;
        rowCount = other.rowCount;
    }
    return *this;
}

DataFrame& DataFrame::operator=(DataFrame&& other) noexcept {
    if (this != &other) {
        columns = std::move(other.columns);
        index = std::move(other.index);
        rowCount = other.rowCount;
        other.rowCount = 0;
    }
    return *this;
}

void DataFrame::addColumn(const std::string& columnName, const ColumnData& data) {
    size_t newRowCount = std::visit([](auto&& vec) { return vec.size(); }, data);
    if (columns.empty()) {
        rowCount = newRowCount;
        index = std::make_shared<Index>(rowCount);
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

ColumnData& DataFrame::operator[](const std::string& columnName) {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    return columns[columnName];
}

const ColumnData& DataFrame::operator[](const std::string& columnName) const {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    return columns.at(columnName);
}

// Implementation for at(columnName) method
ColumnData DataFrame::at(const std::string& columnName) const {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
    return columns.at(columnName);
}

std::string DataFrame::getColumnName(size_t index) const {
    if (index >= columns.size()) {
        throw std::out_of_range("Column index out of range");
    }
    auto it = columns.begin();
    std::advance(it, index);
    return it->first;
}

const std::map<std::string, ColumnData>& DataFrame::getColumns() const {
    return columns;
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
        ColumnData slicedData;
        
        if (std::holds_alternative<IntColumn>(colData)) {
            const auto& vec = std::get<IntColumn>(colData);
            slicedData = IntColumn(vec.begin() + startRow, vec.begin() + endRow);
        } else if (std::holds_alternative<DoubleColumn>(colData)) {
            const auto& vec = std::get<DoubleColumn>(colData);
            slicedData = DoubleColumn(vec.begin() + startRow, vec.begin() + endRow);
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            const auto& vec = std::get<BoolColumn>(colData);
            slicedData = BoolColumn(vec.begin() + startRow, vec.begin() + endRow);
        } else if (std::holds_alternative<StringColumn>(colData)) {
            const auto& vec = std::get<StringColumn>(colData);
            slicedData = StringColumn(vec.begin() + startRow, vec.begin() + endRow);
        }
        
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
        ColumnData filteredData;
        
        if (std::holds_alternative<IntColumn>(colData)) {
            const auto& vec = std::get<IntColumn>(colData);
            IntColumn newVec;
            for (size_t idx : selectedIndices) {
                newVec.push_back(vec[idx]);
            }
            filteredData = newVec;
        } else if (std::holds_alternative<DoubleColumn>(colData)) {
            const auto& vec = std::get<DoubleColumn>(colData);
            DoubleColumn newVec;
            for (size_t idx : selectedIndices) {
                newVec.push_back(vec[idx]);
            }
            filteredData = newVec;
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            const auto& vec = std::get<BoolColumn>(colData);
            BoolColumn newVec;
            for (size_t idx : selectedIndices) {
                newVec.push_back(vec[idx]);
            }
            filteredData = newVec;
        } else if (std::holds_alternative<StringColumn>(colData)) {
            const auto& vec = std::get<StringColumn>(colData);
            StringColumn newVec;
            for (size_t idx : selectedIndices) {
                newVec.push_back(vec[idx]);
            }
            filteredData = newVec;
        }
        
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
    
    if (std::holds_alternative<IntColumn>(colData)) {
        auto& vec = std::get<IntColumn>(colData);
        std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
            // Handle NA values
            bool na1 = vec[i1].is_na();
            bool na2 = vec[i2].is_na();
            
            if (na1 && na2) return false;  // NAs are equal
            if (na1) return !ascending;    // NA is greater/less than non-NA based on ascending
            if (na2) return ascending;     // non-NA is less/greater than NA based on ascending
            
            // Both are non-NA
            if (ascending)
                return vec[i1].value_unsafe() < vec[i2].value_unsafe();
            else
                return vec[i1].value_unsafe() > vec[i2].value_unsafe();
        });
    } else if (std::holds_alternative<DoubleColumn>(colData)) {
        auto& vec = std::get<DoubleColumn>(colData);
        std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
            // Handle NA values
            bool na1 = vec[i1].is_na();
            bool na2 = vec[i2].is_na();
            
            if (na1 && na2) return false;  // NAs are equal
            if (na1) return !ascending;    // NA is greater/less than non-NA based on ascending
            if (na2) return ascending;     // non-NA is less/greater than NA based on ascending
            
            // Both are non-NA
            if (ascending)
                return vec[i1].value_unsafe() < vec[i2].value_unsafe();
            else
                return vec[i1].value_unsafe() > vec[i2].value_unsafe();
        });
    } else if (std::holds_alternative<BoolColumn>(colData)) {
        auto& vec = std::get<BoolColumn>(colData);
        std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
            // Handle NA values
            bool na1 = vec[i1].is_na();
            bool na2 = vec[i2].is_na();
            
            if (na1 && na2) return false;  // NAs are equal
            if (na1) return !ascending;    // NA is greater/less than non-NA based on ascending
            if (na2) return ascending;     // non-NA is less/greater than NA based on ascending
            
            // Both are non-NA
            if (ascending)
                return vec[i1].value_unsafe() < vec[i2].value_unsafe();
            else
                return vec[i1].value_unsafe() > vec[i2].value_unsafe();
        });
    } else if (std::holds_alternative<StringColumn>(colData)) {
        auto& vec = std::get<StringColumn>(colData);
        std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
            // Handle NA values
            bool na1 = !vec[i1].has_value();
            bool na2 = !vec[i2].has_value();
            
            if (na1 && na2) return false;  // NAs are equal
            if (na1) return !ascending;    // NA is greater/less than non-NA based on ascending
            if (na2) return ascending;     // non-NA is less/greater than NA based on ascending
            
            // Both are non-NA
            if (ascending)
                return vec[i1].value() < vec[i2].value();
            else
                return vec[i1].value() > vec[i2].value();
        });
    }

    // Create new index based on sorted indices
    std::vector<std::string> newIndexLabels;
    newIndexLabels.reserve(rowCount);
    for (size_t i = 0; i < rowCount; ++i) {
        newIndexLabels.push_back(index->at(indices[i]));
    }
    index = std::make_shared<Index>(newIndexLabels);

    // Sort all columns
    for (auto& [_, colData] : columns) {
        std::visit(
            [&](auto&& vec) {
                using ValueType = typename std::decay_t<decltype(vec)>::value_type;
                std::vector<ValueType> sortedVec(rowCount);
                for (size_t i = 0; i < rowCount; ++i) {
                    sortedVec[i] = vec[indices[i]];
                }
                vec = std::move(sortedVec);
            },
            colData);
    }
}

void DataFrame::fillna(const ColumnData& value) {
    for (auto& [colName, colData] : columns) {
        std::visit(
            [&](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                
                // Check if the fill value type matches this column's type
                if (!std::holds_alternative<VecType>(value)) {
                    return; // Skip if types don't match
                }
                
                const auto& fillVec = std::get<VecType>(value);
                if (fillVec.empty()) {
                    return; // Skip if no fill value provided
                }
                
                auto fillValue = fillVec[0];
                
                // Fill NA values based on type
                if constexpr (std::is_same_v<VecType, IntColumn>) {
                    for (auto& elem : vec) {
                        if (elem.is_na()) {
                            elem = fillValue;
                        }
                    }
                } else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
                    for (auto& elem : vec) {
                        if (elem.is_na()) {
                            elem = fillValue;
                        }
                    }
                } else if constexpr (std::is_same_v<VecType, StringColumn>) {
                    for (auto& elem : vec) {
                        if (!elem.has_value()) {
                            elem = fillValue;
                        }
                    }
                } else if constexpr (std::is_same_v<VecType, BoolColumn>) {
                    for (auto& elem : vec) {
                        if (elem.is_na()) {
                            elem = fillValue;
                        }
                    }
                }
            },
            colData);
    }
}

DataFrame DataFrame::describe() const {
    // Create a DF to hold the statistics
    DataFrame result;
    
    // Get all numerical columns
    std::vector<std::string> numericColumns;
    for (const auto& [colName, colData] : columns) {
        if (std::holds_alternative<IntColumn>(colData) || 
            std::holds_alternative<DoubleColumn>(colData)) {
            numericColumns.push_back(colName);
        }
    }
    
    if (numericColumns.empty()) {
        return result; // Return empty DataFrame if no numeric columns
    }
    
    // Create the stats rows
    IntColumn countCol;
    DoubleColumn meanCol, stdCol, minCol, q25Col, medianCol, q75Col, maxCol;
    
    // Fill stats for each column
    for (const auto& colName : numericColumns) {
        try {
            auto stats = stats::describe(*this, colName);
            countCol.push_back(NullableInt(static_cast<int>(stats.count)));
            meanCol.push_back(NullableDouble(stats.mean));
            stdCol.push_back(NullableDouble(stats.std));
            minCol.push_back(NullableDouble(stats.min));
            q25Col.push_back(NullableDouble(stats.q25));
            medianCol.push_back(NullableDouble(stats.median));
            q75Col.push_back(NullableDouble(stats.q75));
            maxCol.push_back(NullableDouble(stats.max));
        } catch (const std::exception& e) {
            // If stats computation fails, add NA values
            countCol.push_back(NA_VALUE);
            meanCol.push_back(NA_VALUE);
            stdCol.push_back(NA_VALUE);
            minCol.push_back(NA_VALUE);
            q25Col.push_back(NA_VALUE);
            medianCol.push_back(NA_VALUE);
            q75Col.push_back(NA_VALUE);
            maxCol.push_back(NA_VALUE);
        }
    }
    
    // Create the result DataFrame
    result.addColumn("count", countCol);
    result.addColumn("mean", meanCol);
    result.addColumn("std", stdCol);
    result.addColumn("min", minCol);
    result.addColumn("25%", q25Col);
    result.addColumn("50%", medianCol);
    result.addColumn("75%", q75Col);
    result.addColumn("max", maxCol);
    
    // Set the column names as index
    std::vector<std::string> indexLabels = numericColumns;
    result.setIndex(indexLabels);
    
    return result;
}

void DataFrame::setIndex(const std::vector<std::string>& labels) {
    if (labels.size() != rowCount) {
        throw std::invalid_argument("Index size must match the number of rows");
    }
    
    index = std::make_shared<Index>(labels);
}

std::shared_ptr<Index> DataFrame::getIndex() const {
    return index;
}

void DataFrame::resetIndex(bool drop) {
    if (!drop) {
        // Create a new column from the current index
        StringColumn indexCol;
        for (size_t i = 0; i < rowCount; ++i) {
            indexCol.push_back(NullableString(index->at(i)));
        }
        addColumn("index", indexCol);
    }
    
    // Create new default index
    index = std::make_shared<Index>(rowCount);
}

std::pair<size_t, size_t> DataFrame::shape() const {
    return {rowCount, columns.size()};
}

std::vector<std::string> DataFrame::getColumnNames() const {
    std::vector<std::string> result;
    result.reserve(columns.size());
    for (const auto& [colName, _] : columns) {
        result.push_back(colName);
    }
    return result;
}

bool DataFrame::empty() const {
    return rowCount == 0 || columns.empty();
}

void DataFrame::insert(size_t loc, const std::string& columnName, const ColumnData& value) {
    if (columnExists(columnName)) {
        throw std::invalid_argument("Column already exists: " + columnName);
    }
    
    size_t valueSize = std::visit([](auto&& vec) { return vec.size(); }, value);
    
    if (columns.empty()) {
        rowCount = valueSize;
        index = std::make_shared<Index>(rowCount);
    } else if (valueSize != rowCount) {
        throw std::invalid_argument("Column length must match DataFrame row count");
    }
    
    // Since std::map is ordered, we need to rebuild it with the new column at the right position
    std::map<std::string, ColumnData> newColumns;
    
    size_t i = 0;
    for (const auto& [colName, colData] : columns) {
        if (i == loc) {
            newColumns[columnName] = value;
        }
        newColumns[colName] = colData;
        i++;
    }
    
    // If the column should be inserted at the end
    if (i <= loc) {
        newColumns[columnName] = value;
    }
    
    columns = std::move(newColumns);
}

// Stats methods implementations delegating to stats.cpp

Value DataFrame::sum(const std::string& columnName) const {
    return stats::sum(*this, columnName);
}

Value DataFrame::mean(const std::string& columnName) const {
    return stats::mean(*this, columnName);
}

Value DataFrame::min(const std::string& columnName) const {
    return stats::min(*this, columnName);
}

Value DataFrame::max(const std::string& columnName) const {
    return stats::max(*this, columnName);
}

Value DataFrame::median(const std::string& columnName) const {
    return stats::median(*this, columnName);
}

Value DataFrame::std(const std::string& columnName, size_t ddof) const {
    return stats::std(*this, columnName, ddof);
}

Value DataFrame::var(const std::string& columnName, size_t ddof) const {
    return stats::var(*this, columnName, ddof);
}

Value DataFrame::count(const std::string& columnName) const {
    return stats::count(*this, columnName);
}

DataFrame DataFrame::corr() const {
    return stats::corr(*this);
}

DataFrame DataFrame::cov() const {
    return stats::cov(*this);
}

// Math methods implementations delegating to math.cpp

DataFrame DataFrame::add(const DataFrame& other, const Value& fill_value) const {
    return math::add(*this, other, fill_value);
}

DataFrame DataFrame::sub(const DataFrame& other, const Value& fill_value) const {
    return math::subtract(*this, other, fill_value);
}

DataFrame DataFrame::mul(const DataFrame& other, const Value& fill_value) const {
    return math::multiply(*this, other, fill_value);
}

DataFrame DataFrame::div(const DataFrame& other, const Value& fill_value) const {
    return math::divide(*this, other, fill_value);
}

template<typename T>
DataFrame DataFrame::add(const T& scalar) const {
    return math::add(*this, scalar);
}

template<typename T>
DataFrame DataFrame::sub(const T& scalar) const {
    return math::subtract(*this, scalar);
}

template<typename T>
DataFrame DataFrame::mul(const T& scalar) const {
    return math::multiply(*this, scalar);
}

template<typename T>
DataFrame DataFrame::div(const T& scalar) const {
    return math::divide(*this, scalar);
}

// Template instantiations for common types to avoid linker errors
template DataFrame DataFrame::add<int>(const int& scalar) const;
template DataFrame DataFrame::add<double>(const double& scalar) const;
template DataFrame DataFrame::sub<int>(const int& scalar) const;
template DataFrame DataFrame::sub<double>(const double& scalar) const;
template DataFrame DataFrame::mul<int>(const int& scalar) const;
template DataFrame DataFrame::mul<double>(const double& scalar) const;
template DataFrame DataFrame::div<int>(const int& scalar) const;
template DataFrame DataFrame::div<double>(const double& scalar) const;

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
                    using VecType = std::decay_t<decltype(vec)>;
                    
                    if constexpr (std::is_same_v<VecType, IntColumn>) {
                        if (vec[i].is_na()) std::cout << "NA";
                        else std::cout << vec[i].value_unsafe();
                    } else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
                        if (vec[i].is_na()) std::cout << "NA";
                        else std::cout << vec[i].value_unsafe();
                    } else if constexpr (std::is_same_v<VecType, BoolColumn>) {
                        if (vec[i].is_na()) std::cout << "NA";
                        else std::cout << (vec[i].value_unsafe() ? "true" : "false");
                    } else if constexpr (std::is_same_v<VecType, StringColumn>) {
                        if (!vec[i].has_value()) std::cout << "NA";
                        else std::cout << vec[i].value();
                    }
                    std::cout << "\t";
                },
                colData);
        }
        std::cout << "\n";
    }
}

// Implementation for iloc(row) method
std::map<std::string, Value> DataFrame::iloc(size_t row) const {
    if (row >= rowCount) {
        throw std::out_of_range("Row index out of range");
    }
    
    std::map<std::string, Value> rowData;
    for (const auto& [colName, colData] : columns) {
        rowData[colName] = std::visit(
            [row](auto&& vec) -> Value {
                return vec[row];
            },
            colData);
    }
    
    return rowData;
}

// Implementation for operator[](index) method
ColumnData DataFrame::operator[](size_t index) const {
    if (index >= columns.size()) {
        throw std::out_of_range("Column index out of range");
    }
    
    auto it = columns.begin();
    std::advance(it, index);
    return it->second;
}

// Add info method implementation
void DataFrame::info() const {
    std::cout << "DataFrame information:" << std::endl;
    std::cout << "Size: " << numRows() << " rows × " << numColumns() << " columns" << std::endl;

    std::cout << "\nColumns:" << std::endl;
    for (const auto& colName : getColumnNames()) {
        std::cout << "  - " << colName;
        // Determine column type
        if (std::holds_alternative<IntColumn>(columns.at(colName))) {
            std::cout << " (IntColumn)";
        } else if (std::holds_alternative<DoubleColumn>(columns.at(colName))) {
            std::cout << " (DoubleColumn)";
        } else if (std::holds_alternative<BoolColumn>(columns.at(colName))) {
            std::cout << " (BoolColumn)";
        } else if (std::holds_alternative<StringColumn>(columns.at(colName))) {
            std::cout << " (StringColumn)";
        }
        std::cout << std::endl;
    }
}

} // namespace df