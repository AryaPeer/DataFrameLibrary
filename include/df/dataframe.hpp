#ifndef DF_DS_LIBRARY_DATAFRAME_H
#define DF_DS_LIBRARY_DATAFRAME_H

#include "df/types.hpp"
#include <vector>
#include <string>
#include <map>
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
class GroupBy;

class DataFrame {
private:
    std::map<std::string, ColumnData> columns;
    std::shared_ptr<Index> index;
    size_t rowCount;

public:
    // Constructors
    DataFrame();
    DataFrame(const std::map<std::string, ColumnData>& data);
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
    std::map<std::string, Value> iloc(size_t row) const;

    // Data retrieval
    std::string getColumnName(size_t index) const;
    const std::map<std::string, ColumnData>& getColumns() const;
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
    DataFrame loc(const std::vector<size_t>& indices) const;
    DataFrame head(size_t n = 5) const;
    DataFrame tail(size_t n = 5) const;
    DataFrame sample(size_t n = 1, bool replace = false, size_t seed = 0) const;
    DataFrame nlargest(size_t n, const std::string& columnName) const;
    DataFrame nsmallest(size_t n, const std::string& columnName) const;

    // Column operations
    DataFrame select(const std::vector<std::string>& columnNames) const;
    ColumnData operator[](size_t index) const; // Get column by index
    DataFrame drop(const std::vector<std::string>& columns, bool axis = 0) const;
    
    // Filtering
    DataFrame filter(const std::function<bool(const std::map<std::string, ColumnData>&, size_t)>& condition) const;
    DataFrame query(const std::string& expr) const;

    // Data manipulation
    void sort(const std::string& columnName, bool ascending = true);
    void sort_values(const std::vector<std::string>& columns, const std::vector<bool>& ascending);
    void sort_index(bool ascending = true);
    void fillna(const ColumnData& value);
    DataFrame replace(const Value& old_value, const Value& new_value) const;

    // Data analysis
    GroupBy groupby(const std::vector<std::string>& by);
    DataFrame pivot(const std::string& index, const std::string& columns, const std::string& values) const;
    
    // Information and display
    void info() const;
    void display(size_t n = 5) const;
    std::string to_string(size_t n = 10) const;
    bool empty() const;

    // Transformations
    DataFrame astype(const std::map<std::string, DataType>& types) const;
    DataFrame apply(const std::function<Value(const std::vector<Value>&)>& func, bool axis = 0) const;
    DataFrame transform(const std::function<ColumnData(const ColumnData&)>& func) const;

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

    // Combining DataFrames
    DataFrame merge(const DataFrame& right, const std::string& how = "inner",
                    const std::vector<std::string>& on = {},
                    const std::vector<std::string>& left_on = {},
                    const std::vector<std::string>& right_on = {}) const;
    DataFrame join(const DataFrame& other, const std::string& on = "", 
                   const std::string& how = "left") const;
    DataFrame concat(const std::vector<DataFrame>& frames, bool axis = 0) const;
    
    // I/O operations
    void to_csv(const std::string& filename) const;
    static DataFrame read_csv(const std::string& filename);
    void to_json(const std::string& filename) const;
    static DataFrame read_json(const std::string& filename);
    
    // Set operations
    DataFrame drop_duplicates(const std::vector<std::string>& subset = {}) const;
    DataFrame unique() const;
    bool equals(const DataFrame& other) const;
};

} // namespace df

#endif // DF_DS_LIBRARY_DATAFRAME_H