#ifndef DF_DS_LIBRARY_DATAFRAME_H
#define DF_DS_LIBRARY_DATAFRAME_H

#include <vector>
#include <string>
#include <map>
#include <stdexcept>
#include <variant>
#include <functional>

class DataFrame {
private:
    using ColumnData = std::variant<std::vector<int>, std::vector<double>, std::vector<std::string>, std::vector<bool>>;
    std::map<std::string, ColumnData> columns;
    size_t rowCount;

public:
    DataFrame();
    DataFrame(const std::map<std::string, ColumnData>& data);
    ~DataFrame();

    void addColumn(const std::string& columnName, const ColumnData& data);
    void removeColumn(const std::string& columnName);
    bool columnExists(const std::string& columnName) const;
    ColumnData& operator[](const std::string& columnName);

    size_t numRows() const;
    size_t numColumns() const;
    DataFrame operator()(size_t startRow, size_t endRow) const;
    DataFrame head(size_t n) const;
    DataFrame tail(size_t n) const;

    DataFrame select(const std::vector<std::string>& columnNames) const;
    DataFrame filter(const std::function<bool(const std::map<std::string, ColumnData>&, size_t)>& condition) const;

    void sort(const std::string& columnName, bool ascending = true);
    void fillna(const ColumnData& value);

    double mean(const std::string& columnName) const;
    double sum(const std::string& columnName) const;
    double max(const std::string& columnName) const;
    double min(const std::string& columnName) const;

    void add(const std::string& columnName, double value);
    void subtract(const std::string& columnName, double value);
    void multiply(const std::string& columnName, double value);
    void divide(const std::string& columnName, double value);

    void display(size_t n = 5) const;

    static DataFrame read_csv(const std::string& filename);
    void to_csv(const std::string& filename) const;
};