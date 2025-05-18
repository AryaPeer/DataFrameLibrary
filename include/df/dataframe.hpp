#ifndef DF_DS_LIBRARY_DATAFRAME_H
#define DF_DS_LIBRARY_DATAFRAME_H

#include "df/types.hpp"
#include "df/index.hpp"
#include "df/io.hpp"
#include <vector>
#include <string>
#include <unordered_map>
#include <functional>

namespace df {

class DataFrame {
public:
    using ColumnStore = std::vector<std::pair<std::string, ColumnData>>;

private:
    ColumnStore columns;
    std::unordered_map<std::string, size_t> columnIndex;
    Index index;
    size_t rowCount;

public:
    DataFrame();
    DataFrame(const std::vector<std::pair<std::string, ColumnData>>& data);

    void addColumn(const std::string& columnName, const ColumnData& data);
    void removeColumn(const std::string& columnName);
    bool columnExists(const std::string& columnName) const;
    ColumnData& operator[](const std::string& columnName);
    const ColumnData& operator[](const std::string& columnName) const;
    ColumnData at(const std::string& columnName) const;

    std::string getColumnName(size_t idx) const;
    const ColumnStore& getColumns() const;
    std::vector<std::string> getColumnNames() const;
    const Index& getIndex() const;
    void setIndex(const std::vector<std::string>& labels);

    size_t numRows() const;
    size_t numColumns() const;
    std::pair<size_t, size_t> shape() const;

    DataFrame operator()(size_t startRow, size_t endRow) const;
    DataFrame head(size_t n = 5) const;
    DataFrame tail(size_t n = 5) const;

    DataFrame select(const std::vector<std::string>& columnNames) const;
    ColumnData operator[](size_t idx) const;

    DataFrame filter(const std::function<bool(const ColumnStore&, size_t)>& condition) const;

    void sort(const std::string& columnName, bool ascending = true);
    void fillna(const Value& value);

    void info() const;
    void display(size_t n = 5) const;
    bool empty() const;

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

    DataFrame add(const DataFrame& other, const Value& fillValue = NA_VALUE) const;
    DataFrame sub(const DataFrame& other, const Value& fillValue = NA_VALUE) const;
    DataFrame mul(const DataFrame& other, const Value& fillValue = NA_VALUE) const;
    DataFrame div(const DataFrame& other, const Value& fillValue = NA_VALUE) const;

    template<typename T>
    DataFrame add(const T& scalar) const;
    template<typename T>
    DataFrame sub(const T& scalar) const;
    template<typename T>
    DataFrame mul(const T& scalar) const;
    template<typename T>
    DataFrame div(const T& scalar) const;

    void toCSV(const std::string& filename, const io::CSVWriteOptions& options = {}) const;
    static DataFrame readCSV(const std::string& filename, const io::CSVReadOptions& options = {});
};

} // namespace df

#endif // DF_DS_LIBRARY_DATAFRAME_H
