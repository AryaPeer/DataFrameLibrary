#include "df/dataframe.hpp"
#include "df/index.hpp"
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

DataFrame::DataFrame(const std::map<std::string, ColumnData>& data) {
    for (const auto& [name, col] : data) {
        columnIndex[name] = columns.size();
        columns.emplace_back(name, col);
    }

    if (!columns.empty()) {
        rowCount = std::visit([](auto&& vec) { return vec.size(); }, columns[0].second);

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

DataFrame::DataFrame(const std::vector<std::pair<std::string, ColumnData>>& data) {
    for (const auto& [name, col] : data) {
        if (columnIndex.count(name)) {
            throw std::invalid_argument("Duplicate column name: " + name);
        }
        columnIndex[name] = columns.size();
        columns.emplace_back(name, col);
    }

    if (!columns.empty()) {
        rowCount = std::visit([](auto&& vec) { return vec.size(); }, columns[0].second);

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
    : columns(other.columns), columnIndex(other.columnIndex),
      index(std::make_shared<Index>(*other.index)), rowCount(other.rowCount) {}

DataFrame::DataFrame(DataFrame&& other) noexcept
    : columns(std::move(other.columns)), columnIndex(std::move(other.columnIndex)),
      index(std::move(other.index)), rowCount(other.rowCount) {
    other.rowCount = 0;
    other.index = std::make_shared<Index>(0);
}

DataFrame::~DataFrame() {}

DataFrame& DataFrame::operator=(const DataFrame& other) {
    if (this != &other) {
        columns = other.columns;
        columnIndex = other.columnIndex;
        index = std::make_shared<Index>(*other.index);
        rowCount = other.rowCount;
    }
    return *this;
}

DataFrame& DataFrame::operator=(DataFrame&& other) noexcept {
    if (this != &other) {
        columns = std::move(other.columns);
        columnIndex = std::move(other.columnIndex);
        index = std::move(other.index);
        rowCount = other.rowCount;
        other.rowCount = 0;
        other.index = std::make_shared<Index>(0);
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

    auto it = columnIndex.find(columnName);
    if (it != columnIndex.end()) {
        columns[it->second].second = data;
    } else {
        columnIndex[columnName] = columns.size();
        columns.emplace_back(columnName, data);
    }
}

void DataFrame::removeColumn(const std::string& columnName) {
    auto it = columnIndex.find(columnName);
    if (it == columnIndex.end()) return;
    size_t pos = it->second;
    columns.erase(columns.begin() + pos);
    columnIndex.erase(it);
    for (auto& [name, idx] : columnIndex) {
        if (idx > pos) --idx;
    }
    if (columns.empty()) {
        rowCount = 0;
        index = std::make_shared<Index>(0);
    }
}

bool DataFrame::columnExists(const std::string& columnName) const {
    return columnIndex.find(columnName) != columnIndex.end();
}

ColumnData& DataFrame::operator[](const std::string& columnName) {
    auto it = columnIndex.find(columnName);
    if (it == columnIndex.end()) {
        throw std::out_of_range("Column does not exist.");
    }
    return columns[it->second].second;
}

const ColumnData& DataFrame::operator[](const std::string& columnName) const {
    auto it = columnIndex.find(columnName);
    if (it == columnIndex.end()) {
        throw std::out_of_range("Column does not exist.");
    }
    return columns[it->second].second;
}

ColumnData DataFrame::at(const std::string& columnName) const {
    auto it = columnIndex.find(columnName);
    if (it == columnIndex.end()) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
    return columns[it->second].second;
}

std::string DataFrame::getColumnName(size_t idx) const {
    if (idx >= columns.size()) {
        throw std::out_of_range("Column index out of range");
    }
    return columns[idx].first;
}

const DataFrame::ColumnStore& DataFrame::getColumns() const {
    return columns;
}

size_t DataFrame::numRows() const {
    return rowCount;
}

size_t DataFrame::numColumns() const {
    return columns.size();
}

DataFrame DataFrame::operator()(size_t startRow, size_t endRow) const {
    if (startRow > endRow || endRow > rowCount) {
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

    slicedDF.setIndex(index->slice(startRow, endRow).getLabels());

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
        auto it = columnIndex.find(colName);
        if (it != columnIndex.end()) {
            selectedDF.addColumn(colName, columns[it->second].second);
        } else {
            throw std::out_of_range("Column does not exist: " + colName);
        }
    }

    if (!selectedDF.empty()) {
        selectedDF.setIndex(index->getLabels());
    }

    return selectedDF;
}

DataFrame DataFrame::filter(const std::function<bool(const ColumnStore&, size_t)>& condition) const {
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

    if (!selectedIndices.empty()) {
        filteredDF.setIndex(index->take(selectedIndices).getLabels());
    }

    return filteredDF;
}

void DataFrame::sort(const std::string& columnName, bool ascending) {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }

    std::vector<size_t> indices(rowCount);
    std::iota(indices.begin(), indices.end(), 0);

    auto& colData = columns[columnIndex.at(columnName)].second;

    if (std::holds_alternative<IntColumn>(colData)) {
        auto& vec = std::get<IntColumn>(colData);

        std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
            bool na1 = vec[i1].is_na();
            bool na2 = vec[i2].is_na();

            if (na1 && na2) return false;
            if (na1) return false;  // NA always goes to end
            if (na2) return true;   // non-NA always before NA

            if (ascending)
                return vec[i1].value_unsafe() < vec[i2].value_unsafe();
            else
                return vec[i1].value_unsafe() > vec[i2].value_unsafe();
        });

    } else if (std::holds_alternative<DoubleColumn>(colData)) {
        auto& vec = std::get<DoubleColumn>(colData);

        std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
            bool na1 = vec[i1].is_na();
            bool na2 = vec[i2].is_na();

            if (na1 && na2) return false;
            if (na1) return false;  // NA always goes to end
            if (na2) return true;   // non-NA always before NA

            if (ascending)
                return vec[i1].value_unsafe() < vec[i2].value_unsafe();
            else
                return vec[i1].value_unsafe() > vec[i2].value_unsafe();
        });

    } else if (std::holds_alternative<BoolColumn>(colData)) {
        auto& vec = std::get<BoolColumn>(colData);

        std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
            bool na1 = vec[i1].is_na();
            bool na2 = vec[i2].is_na();

            if (na1 && na2) return false;
            if (na1) return false;  // NA always goes to end
            if (na2) return true;   // non-NA always before NA

            if (ascending)
                return vec[i1].value_unsafe() < vec[i2].value_unsafe();
            else
                return vec[i1].value_unsafe() > vec[i2].value_unsafe();
        });

    } else if (std::holds_alternative<StringColumn>(colData)) {
        auto& vec = std::get<StringColumn>(colData);

        std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
            bool na1 = vec[i1].is_na();
            bool na2 = vec[i2].is_na();

            if (na1 && na2) return false;
            if (na1) return false;  // NA always goes to end
            if (na2) return true;   // non-NA always before NA

            if (ascending)
                return vec[i1].value_unsafe() < vec[i2].value_unsafe();
            else
                return vec[i1].value_unsafe() > vec[i2].value_unsafe();
        });
    }

    std::vector<std::string> newIndexLabels;
    newIndexLabels.reserve(rowCount);

    for (size_t i = 0; i < rowCount; ++i) {
        newIndexLabels.push_back(index->at(indices[i]));
    }

    index = std::make_shared<Index>(newIndexLabels);

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

void DataFrame::fillna(const Value& value) {
    for (auto& [colName, colData] : columns) {
        if (std::holds_alternative<IntColumn>(colData)) {
            int fillVal;
            bool hasFill = false;
            if (std::holds_alternative<int>(value)) {
                fillVal = std::get<int>(value); hasFill = true;
            } else if (std::holds_alternative<NullableInt>(value)) {
                const auto& nv = std::get<NullableInt>(value);
                if (!nv.is_na()) { fillVal = nv.value_unsafe(); hasFill = true; }
            }
            if (hasFill) {
                auto& vec = std::get<IntColumn>(colData);
                for (auto& elem : vec) {
                    if (elem.is_na()) elem = fillVal;
                }
            }
        } else if (std::holds_alternative<DoubleColumn>(colData)) {
            double fillVal;
            bool hasFill = false;
            if (std::holds_alternative<double>(value)) {
                fillVal = std::get<double>(value); hasFill = true;
            } else if (std::holds_alternative<int>(value)) {
                fillVal = static_cast<double>(std::get<int>(value)); hasFill = true;
            } else if (std::holds_alternative<NullableDouble>(value)) {
                const auto& nv = std::get<NullableDouble>(value);
                if (!nv.is_na()) { fillVal = nv.value_unsafe(); hasFill = true; }
            } else if (std::holds_alternative<NullableInt>(value)) {
                const auto& nv = std::get<NullableInt>(value);
                if (!nv.is_na()) { fillVal = static_cast<double>(nv.value_unsafe()); hasFill = true; }
            }
            if (hasFill) {
                auto& vec = std::get<DoubleColumn>(colData);
                for (auto& elem : vec) {
                    if (elem.is_na()) elem = fillVal;
                }
            }
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            bool fillVal;
            bool hasFill = false;
            if (std::holds_alternative<bool>(value)) {
                fillVal = std::get<bool>(value); hasFill = true;
            } else if (std::holds_alternative<NullableBool>(value)) {
                const auto& nv = std::get<NullableBool>(value);
                if (!nv.is_na()) { fillVal = nv.value_unsafe(); hasFill = true; }
            }
            if (hasFill) {
                auto& vec = std::get<BoolColumn>(colData);
                for (auto& elem : vec) {
                    if (elem.is_na()) elem = fillVal;
                }
            }
        } else if (std::holds_alternative<StringColumn>(colData)) {
            std::string fillVal;
            bool hasFill = false;
            if (std::holds_alternative<std::string>(value)) {
                fillVal = std::get<std::string>(value); hasFill = true;
            } else if (std::holds_alternative<NullableString>(value)) {
                const auto& nv = std::get<NullableString>(value);
                if (!nv.is_na()) { fillVal = nv.value_unsafe(); hasFill = true; }
            }
            if (hasFill) {
                auto& vec = std::get<StringColumn>(colData);
                for (auto& elem : vec) {
                    if (elem.is_na()) elem = fillVal;
                }
            }
        }
    }
}

DataFrame DataFrame::describe() const {
    DataFrame result;

    std::vector<std::string> numericColumns;

    for (const auto& [colName, colData] : columns) {
        if (std::holds_alternative<IntColumn>(colData) ||
            std::holds_alternative<DoubleColumn>(colData)) {
            numericColumns.push_back(colName);
        }
    }

    if (numericColumns.empty()) {
        return result;
    }

    std::vector<std::string> statNames = {
        "count", "mean", "std", "min", "25%", "50%", "75%", "max"
    };

    for (const auto& colName : numericColumns) {
        DoubleColumn statCol;

        try {
            auto s = stats::describe(*this, colName);
            statCol.push_back(NullableDouble(s.count));
            statCol.push_back(NullableDouble(s.mean));
            statCol.push_back(NullableDouble(s.std));
            statCol.push_back(NullableDouble(s.min));
            statCol.push_back(NullableDouble(s.q25));
            statCol.push_back(NullableDouble(s.median));
            statCol.push_back(NullableDouble(s.q75));
            statCol.push_back(NullableDouble(s.max));
        } catch (const std::exception&) {
            for (size_t i = 0; i < statNames.size(); ++i) {
                statCol.push_back(NA_VALUE);
            }
        }

        result.addColumn(colName, statCol);
    }

    result.setIndex(statNames);

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
        StringColumn indexCol;

        for (size_t i = 0; i < rowCount; ++i) {
            indexCol.push_back(NullableString(index->at(i)));
        }

        std::string colName = "index";
        int suffix = 0;
        while (columnExists(colName)) {
            colName = "index_" + std::to_string(suffix++);
        }
        addColumn(colName, indexCol);
    }

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

    size_t newRowCount = std::visit([](auto&& vec) { return vec.size(); }, value);
    if (columns.empty()) {
        rowCount = newRowCount;
        index = std::make_shared<Index>(rowCount);
    } else if (newRowCount != rowCount) {
        throw std::invalid_argument("All columns must have the same number of rows.");
    }

    if (loc > columns.size()) {
        loc = columns.size();
    }

    columns.insert(columns.begin() + loc, {columnName, value});
    for (size_t i = loc; i < columns.size(); ++i) {
        columnIndex[columns[i].first] = i;
    }
}

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

    std::cout << "\t";
    for (const auto& [colName, _] : columns) {
        std::cout << colName << "\t";
    }
    std::cout << "\n";

    for (size_t i = 0; i < displayRows; ++i) {
        std::cout << index->at(i) << "\t";
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
                        if (vec[i].is_na()) std::cout << "NA";
                        else std::cout << vec[i].value_unsafe();
                    }

                    std::cout << "\t";
                },
                colData);
        }
        std::cout << "\n";
    }
}

std::vector<std::pair<std::string, Value>> DataFrame::iloc(size_t row) const {
    if (row >= rowCount) {
        throw std::out_of_range("Row index out of range");
    }

    std::vector<std::pair<std::string, Value>> rowData;
    rowData.reserve(columns.size());

    for (const auto& [colName, colData] : columns) {
        rowData.emplace_back(colName, std::visit(
            [row](auto&& vec) -> Value {
                return vec[row];
            },
            colData));
    }

    return rowData;
}

ColumnData DataFrame::operator[](size_t idx) const {
    if (idx >= columns.size()) {
        throw std::out_of_range("Column index out of range");
    }
    return columns[idx].second;
}

void DataFrame::info() const {
    std::cout << "DataFrame information:" << std::endl;
    std::cout << "Size: " << numRows() << " rows × " << numColumns() << " columns" << std::endl;

    std::cout << "\nColumns:" << std::endl;
    for (const auto& [colName, colData] : columns) {
        std::cout << "  - " << colName;
        if (std::holds_alternative<IntColumn>(colData)) {
            std::cout << " (IntColumn)";
        } else if (std::holds_alternative<DoubleColumn>(colData)) {
            std::cout << " (DoubleColumn)";
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            std::cout << " (BoolColumn)";
        } else if (std::holds_alternative<StringColumn>(colData)) {
            std::cout << " (StringColumn)";
        }
        std::cout << std::endl;
    }
}

void DataFrame::to_csv(const std::string& filename, const io::CSVWriteOptions& options) const {
    io::to_csv(*this, filename, options);
}

DataFrame DataFrame::read_csv(const std::string& filename, const io::CSVReadOptions& options) {
    return io::read_csv(filename, options);
}

} // namespace df
