#ifndef DF_DS_LIBRARY_DATAFRAME_H
#define DF_DS_LIBRARY_DATAFRAME_H

#include "df/types.hpp"
#include "df/io.hpp"
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <stdexcept>
#include <variant>
#include <functional>
#include <memory>
#include <utility>
#include <algorithm>
#include <iostream>
#include <sstream>

namespace df {

class Index;

class DataFrame {
public:
    using ColumnStore = std::vector<std::pair<std::string, ColumnData>>;

private:
    ColumnStore columns;
    std::unordered_map<std::string, size_t> columnIndex;
    std::shared_ptr<Index> index;
    size_t rowCount;

public:
    // Constructors
    DataFrame();
    DataFrame(const std::map<std::string, ColumnData>& data);
    DataFrame(const std::vector<std::pair<std::string, ColumnData>>& data);
    DataFrame(const DataFrame& other);
    DataFrame(DataFrame&& other) noexcept;
    ~DataFrame();

    // Assignment operators
    DataFrame& operator=(const DataFrame& other);
    DataFrame& operator=(DataFrame&& other) noexcept;

    // Column access and manipulation
    void addColumn(const std::string& columnName, const ColumnData& data);
    void removeColumn(const std::string& columnName);
    bool columnExists(const std::string& columnName) const;
    ColumnData& operator[](const std::string& columnName);
    const ColumnData& operator[](const std::string& columnName) const;
    void insert(size_t loc, const std::string& columnName, const ColumnData& value);
    ColumnData at(const std::string& columnName) const;
    std::vector<std::pair<std::string, Value>> iloc(size_t row) const;

    // Data retrieval
    std::string getColumnName(size_t index) const;
    const ColumnStore& getColumns() const;
    std::vector<std::string> getColumnNames() const;
    std::shared_ptr<Index> getIndex() const;
    void setIndex(const std::vector<std::string>& labels);
    void resetIndex(bool drop = false);

    // Shape information
    size_t numRows() const;
    size_t numColumns() const;
    std::pair<size_t, size_t> shape() const;

    // Row operations
    DataFrame operator()(size_t startRow, size_t endRow) const;
    DataFrame head(size_t n = 5) const;
    DataFrame tail(size_t n = 5) const;

    // Column operations
    DataFrame select(const std::vector<std::string>& columnNames) const;
    ColumnData operator[](size_t index) const; // Get column by index

    // Filtering
    DataFrame filter(const std::function<bool(const ColumnStore&, size_t)>& condition) const;

    // Data manipulation
    void sort(const std::string& columnName, bool ascending = true);
    void fillna(const Value& value);
    
    // Information and display
    void info() const;
    void display(size_t n = 5) const;
    bool empty() const;

    // Aggregations and Statistics
    Value sum(const std::string& columnName) const;
    Value mean(const std::string& columnName) const;
    Value min(const std::string& columnName) const;
    Value max(const std::string& columnName) const;
    Value median(const std::string& columnName) const;
    Value std(const std::string& columnName, size_t ddof = 1) const;
    Value var(const std::string& columnName, size_t ddof = 1) const;
    Value count(const std::string& columnName) const;
    DataFrame corr() const;
    DataFrame cov() const;
    DataFrame describe() const;

    // Binary operations
    DataFrame add(const DataFrame& other, const Value& fill_value = NA_VALUE) const;
    DataFrame sub(const DataFrame& other, const Value& fill_value = NA_VALUE) const;
    DataFrame mul(const DataFrame& other, const Value& fill_value = NA_VALUE) const;
    DataFrame div(const DataFrame& other, const Value& fill_value = NA_VALUE) const;
    
    template<typename T>
    DataFrame add(const T& scalar) const;
    template<typename T>
    DataFrame sub(const T& scalar) const;
    template<typename T>
    DataFrame mul(const T& scalar) const;
    template<typename T>
    DataFrame div(const T& scalar) const;

    // I/O operations
    void to_csv(const std::string& filename, const io::CSVWriteOptions& options = {}) const;
    static DataFrame read_csv(const std::string& filename, const io::CSVReadOptions& options = {});
};

} // namespace df

#endif // DF_DS_LIBRARY_DATAFRAME_H