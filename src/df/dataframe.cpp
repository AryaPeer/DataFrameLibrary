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
#include <cmath>
#include <limits>

namespace df {

DataFrame::DataFrame() : index(0), rowCount(0) {}

DataFrame::DataFrame(const std::vector<std::pair<std::string, ColumnData>>& data) : index(0), rowCount(0) {
    for (const auto& [name, col] : data) {
        if (columnIndex.count(name)) {
            throw std::invalid_argument("Duplicate column name: " + name);
        }
        addColumn(name, col);
    }
}

void DataFrame::addColumn(const std::string& columnName, const ColumnData& data) {
    size_t newRowCount = std::visit([](const auto& vec) { return vec.size(); }, data);

    if (columns.empty()) {
        rowCount = newRowCount;
        index = Index(rowCount);
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
        index = Index(0);
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

size_t DataFrame::numRows() const    { return rowCount; }
size_t DataFrame::numColumns() const { return columns.size(); }
std::pair<size_t, size_t> DataFrame::shape() const { return {rowCount, columns.size()}; }

DataFrame DataFrame::operator()(size_t startRow, size_t endRow) const {
    if (startRow > endRow || endRow > rowCount) {
        throw std::out_of_range("Invalid row indices.");
    }
    DataFrame sliced;
    for (const auto& [colName, colData] : columns) {
        ColumnData slicedData = std::visit([startRow, endRow](const auto& vec) -> ColumnData {
            using Vec = std::decay_t<decltype(vec)>;
            return Vec(vec.begin() + startRow, vec.begin() + endRow);
        }, colData);
        sliced.addColumn(colName, slicedData);
    }
    if (!sliced.columns.empty()) {
        sliced.setIndex(index.slice(startRow, endRow).getLabels());
    }
    return sliced;
}

DataFrame DataFrame::head(size_t n) const {
    return this->operator()(0, std::min(n, rowCount));
}

DataFrame DataFrame::tail(size_t n) const {
    size_t start = rowCount > n ? rowCount - n : 0;
    return this->operator()(start, rowCount);
}

DataFrame DataFrame::select(const std::vector<std::string>& columnNames) const {
    DataFrame selected;
    for (const auto& colName : columnNames) {
        auto it = columnIndex.find(colName);
        if (it == columnIndex.end()) {
            throw std::out_of_range("Column does not exist: " + colName);
        }
        selected.addColumn(colName, columns[it->second].second);
    }
    if (!selected.empty()) {
        selected.setIndex(index.getLabels());
    }
    return selected;
}

DataFrame DataFrame::filter(const std::function<bool(const ColumnStore&, size_t)>& condition) const {
    std::vector<size_t> selectedIndices;
    for (size_t i = 0; i < rowCount; ++i) {
        if (condition(columns, i)) selectedIndices.push_back(i);
    }

    DataFrame filtered;
    for (const auto& [colName, colData] : columns) {
        ColumnData filteredData = std::visit([&selectedIndices](const auto& vec) -> ColumnData {
            using Vec = std::decay_t<decltype(vec)>;
            Vec result;
            result.reserve(selectedIndices.size());
            for (size_t idx : selectedIndices) result.push_back(vec[idx]);
            return result;
        }, colData);
        filtered.addColumn(colName, filteredData);
    }

    if (!selectedIndices.empty()) {
        filtered.setIndex(index.take(selectedIndices).getLabels());
    }
    return filtered;
}

void DataFrame::sort(const std::string& columnName, bool ascending) {
    if (!columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }

    std::vector<size_t> indices(rowCount);
    std::iota(indices.begin(), indices.end(), 0);

    const auto& sortColData = columns[columnIndex.at(columnName)].second;
    std::visit([&](const auto& vec) {
        std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) {
            bool na1 = vec[i1].isNA();
            bool na2 = vec[i2].isNA();
            if (na1) return false;
            if (na2) return true;
            return ascending
                ? vec[i1].valueUnsafe() < vec[i2].valueUnsafe()
                : vec[i1].valueUnsafe() > vec[i2].valueUnsafe();
        });
    }, sortColData);

    std::vector<std::string> newLabels;
    newLabels.reserve(rowCount);
    for (size_t i = 0; i < rowCount; ++i) {
        newLabels.push_back(index.at(indices[i]));
    }
    index = Index(newLabels);

    for (auto& [_, colData] : columns) {
        std::visit([&indices, this](auto& vec) {
            using Vec = std::decay_t<decltype(vec)>;
            Vec sorted(rowCount);
            for (size_t i = 0; i < rowCount; ++i) sorted[i] = vec[indices[i]];
            vec = std::move(sorted);
        }, colData);
    }
}

void DataFrame::fillna(const Value& value) {
    for (auto& [colName, colData] : columns) {
        std::visit([&value](auto& vec) {
            using V = typename std::decay_t<decltype(vec)>::value_type;

            if constexpr (std::is_same_v<V, NullableInt>) {
                std::optional<int> fill;
                if (std::holds_alternative<int>(value)) fill = std::get<int>(value);
                else if (std::holds_alternative<NullableInt>(value)) {
                    const auto& n = std::get<NullableInt>(value);
                    if (!n.isNA()) fill = n.valueUnsafe();
                }
                if (fill) for (auto& e : vec) if (e.isNA()) e = *fill;
            }
            else if constexpr (std::is_same_v<V, NullableDouble>) {
                std::optional<double> fill;
                if (std::holds_alternative<double>(value)) fill = std::get<double>(value);
                else if (std::holds_alternative<int>(value)) fill = static_cast<double>(std::get<int>(value));
                else if (std::holds_alternative<NullableDouble>(value)) {
                    const auto& n = std::get<NullableDouble>(value);
                    if (!n.isNA()) fill = n.valueUnsafe();
                } else if (std::holds_alternative<NullableInt>(value)) {
                    const auto& n = std::get<NullableInt>(value);
                    if (!n.isNA()) fill = static_cast<double>(n.valueUnsafe());
                }
                if (fill) for (auto& e : vec) if (e.isNA()) e = *fill;
            }
            else if constexpr (std::is_same_v<V, NullableBool>) {
                std::optional<bool> fill;
                if (std::holds_alternative<bool>(value)) fill = std::get<bool>(value);
                else if (std::holds_alternative<NullableBool>(value)) {
                    const auto& n = std::get<NullableBool>(value);
                    if (!n.isNA()) fill = n.valueUnsafe();
                }
                if (fill) for (auto& e : vec) if (e.isNA()) e = *fill;
            }
            else if constexpr (std::is_same_v<V, NullableString>) {
                std::optional<std::string> fill;
                if (std::holds_alternative<std::string>(value)) fill = std::get<std::string>(value);
                else if (std::holds_alternative<NullableString>(value)) {
                    const auto& n = std::get<NullableString>(value);
                    if (!n.isNA()) fill = n.valueUnsafe();
                }
                if (fill) for (auto& e : vec) if (e.isNA()) e = *fill;
            }
        }, colData);
    }
}

DataFrame DataFrame::describe() const {
    std::vector<std::string> numericColumns;
    for (const auto& [colName, colData] : columns) {
        if (std::holds_alternative<IntColumn>(colData) ||
            std::holds_alternative<DoubleColumn>(colData)) {
            numericColumns.push_back(colName);
        }
    }

    DataFrame result;
    if (numericColumns.empty()) return result;

    const std::vector<std::string> statNames = {"count", "mean", "std", "min", "25%", "50%", "75%", "max"};
    constexpr double NaN = std::numeric_limits<double>::quiet_NaN();

    for (const auto& colName : numericColumns) {
        const auto& colData = columns[columnIndex.at(colName)].second;

        std::vector<double> values;
        std::visit([&values](const auto& vec) {
            using Vec = std::decay_t<decltype(vec)>;
            if constexpr (std::is_same_v<Vec, IntColumn> || std::is_same_v<Vec, DoubleColumn>) {
                for (const auto& v : vec) {
                    if (!v.isNA()) values.push_back(static_cast<double>(v.valueUnsafe()));
                }
            }
        }, colData);

        DoubleColumn statCol;
        if (values.empty()) {
            for (size_t i = 0; i < statNames.size(); ++i) statCol.push_back(NA_VALUE);
        } else {
            double sum = 0.0;
            for (double v : values) sum += v;
            double mean = sum / values.size();

            double sumSq = 0.0;
            for (double v : values) sumSq += (v - mean) * (v - mean);
            double stddev = values.size() > 1
                ? std::sqrt(sumSq / (values.size() - 1))
                : NaN;

            std::vector<double> sorted = values;
            std::sort(sorted.begin(), sorted.end());

            auto interpolate = [&sorted](double q) -> double {
                double pos = q * (sorted.size() - 1);
                size_t lower = static_cast<size_t>(pos);
                size_t upper = lower + 1;
                double frac = pos - lower;
                if (upper >= sorted.size()) return sorted[lower];
                return sorted[lower] * (1.0 - frac) + sorted[upper] * frac;
            };

            statCol.push_back(NullableDouble(static_cast<double>(values.size())));
            statCol.push_back(NullableDouble(mean));
            statCol.push_back(NullableDouble(stddev));
            statCol.push_back(NullableDouble(sorted.front()));
            statCol.push_back(NullableDouble(interpolate(0.25)));
            statCol.push_back(NullableDouble(interpolate(0.50)));
            statCol.push_back(NullableDouble(interpolate(0.75)));
            statCol.push_back(NullableDouble(sorted.back()));
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
    index = Index(labels);
}

const Index& DataFrame::getIndex() const { return index; }

std::vector<std::string> DataFrame::getColumnNames() const {
    std::vector<std::string> result;
    result.reserve(columns.size());
    for (const auto& [colName, _] : columns) result.push_back(colName);
    return result;
}

bool DataFrame::empty() const { return rowCount == 0 || columns.empty(); }

Value DataFrame::sum(const std::string& columnName) const    { return stats::sum(this->at(columnName)); }
Value DataFrame::mean(const std::string& columnName) const   { return stats::mean(this->at(columnName)); }
Value DataFrame::min(const std::string& columnName) const    { return stats::min(this->at(columnName)); }
Value DataFrame::max(const std::string& columnName) const    { return stats::max(this->at(columnName)); }
Value DataFrame::median(const std::string& columnName) const { return stats::median(this->at(columnName)); }
Value DataFrame::std(const std::string& columnName, size_t ddof) const { return stats::std(this->at(columnName), ddof); }
Value DataFrame::var(const std::string& columnName, size_t ddof) const { return stats::var(this->at(columnName), ddof); }
Value DataFrame::count(const std::string& columnName) const  { return stats::count(this->at(columnName)); }

DataFrame DataFrame::corr() const { return stats::corr(*this); }
DataFrame DataFrame::cov() const  { return stats::cov(*this); }

DataFrame DataFrame::add(const DataFrame& other, const Value& fillValue) const {
    return math::add(*this, other, fillValue);
}
DataFrame DataFrame::sub(const DataFrame& other, const Value& fillValue) const {
    return math::subtract(*this, other, fillValue);
}
DataFrame DataFrame::mul(const DataFrame& other, const Value& fillValue) const {
    return math::multiply(*this, other, fillValue);
}
DataFrame DataFrame::div(const DataFrame& other, const Value& fillValue) const {
    return math::divide(*this, other, fillValue);
}

template<typename T>
DataFrame DataFrame::add(const T& scalar) const { return math::add(*this, scalar); }
template<typename T>
DataFrame DataFrame::sub(const T& scalar) const { return math::subtract(*this, scalar); }
template<typename T>
DataFrame DataFrame::mul(const T& scalar) const { return math::multiply(*this, scalar); }
template<typename T>
DataFrame DataFrame::div(const T& scalar) const { return math::divide(*this, scalar); }

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
        std::cout << index.at(i) << "\t";
        for (const auto& [_, colData] : columns) {
            std::visit([i](const auto& vec) {
                using Vec = std::decay_t<decltype(vec)>;
                if (vec[i].isNA()) {
                    std::cout << "NA";
                } else if constexpr (std::is_same_v<Vec, BoolColumn>) {
                    std::cout << (vec[i].valueUnsafe() ? "true" : "false");
                } else {
                    std::cout << vec[i].valueUnsafe();
                }
                std::cout << "\t";
            }, colData);
        }
        std::cout << "\n";
    }
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
        std::visit([](const auto& vec) {
            using Vec = std::decay_t<decltype(vec)>;
            if constexpr (std::is_same_v<Vec, IntColumn>)         std::cout << " (IntColumn)";
            else if constexpr (std::is_same_v<Vec, DoubleColumn>) std::cout << " (DoubleColumn)";
            else if constexpr (std::is_same_v<Vec, BoolColumn>)   std::cout << " (BoolColumn)";
            else if constexpr (std::is_same_v<Vec, StringColumn>) std::cout << " (StringColumn)";
        }, colData);
        std::cout << std::endl;
    }
}

void DataFrame::toCSV(const std::string& filename, const io::CSVWriteOptions& options) const {
    io::toCSV(*this, filename, options);
}

DataFrame DataFrame::readCSV(const std::string& filename, const io::CSVReadOptions& options) {
    return io::readCSV(filename, options);
}

} // namespace df
