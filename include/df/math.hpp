#ifndef DF_DS_LIBRARY_MATH_H
#define DF_DS_LIBRARY_MATH_H

#include "df/types.hpp"
#include <cmath>

namespace df {

// Forward declarations
class DataFrame;

namespace math {

// Basic arithmetic operations on DataFrames
void add_inplace(DataFrame& df, const std::string& columnName, const Value& value);
void subtract_inplace(DataFrame& df, const std::string& columnName, const Value& value);
void multiply_inplace(DataFrame& df, const std::string& columnName, const Value& value);
void divide_inplace(DataFrame& df, const std::string& columnName, const Value& value);

// DataFrame arithmetic operators
DataFrame add(const DataFrame& df, const DataFrame& other, const Value& fill_value = NA_VALUE);
DataFrame subtract(const DataFrame& df, const DataFrame& other, const Value& fill_value = NA_VALUE);
DataFrame multiply(const DataFrame& df, const DataFrame& other, const Value& fill_value = NA_VALUE);
DataFrame divide(const DataFrame& df, const DataFrame& other, const Value& fill_value = NA_VALUE);

// DataFrame-scalar operations
DataFrame add(const DataFrame& df, const Value& value);
DataFrame subtract(const DataFrame& df, const Value& value);
DataFrame multiply(const DataFrame& df, const Value& value);
DataFrame divide(const DataFrame& df, const Value& value);

// Column arithmetic operations
ColumnData add(const ColumnData& col, const ColumnData& other, const Value& fill_value = NA_VALUE);
ColumnData subtract(const ColumnData& col, const ColumnData& other, const Value& fill_value = NA_VALUE);
ColumnData multiply(const ColumnData& col, const ColumnData& other, const Value& fill_value = NA_VALUE);
ColumnData divide(const ColumnData& col, const ColumnData& other, const Value& fill_value = NA_VALUE);

// Column-scalar operations
ColumnData add(const ColumnData& col, const Value& value);
ColumnData subtract(const ColumnData& col, const Value& value);
ColumnData multiply(const ColumnData& col, const Value& value);
ColumnData divide(const ColumnData& col, const Value& value);

// Element-wise math functions for Columns
ColumnData abs(const ColumnData& col);
ColumnData sqrt(const ColumnData& col);
ColumnData square(const ColumnData& col);
ColumnData log(const ColumnData& col);
ColumnData log10(const ColumnData& col);
ColumnData exp(const ColumnData& col);

// Element-wise comparison operations for Column data
ColumnData gt(const ColumnData& col, const Value& value);
ColumnData lt(const ColumnData& col, const Value& value);
ColumnData ge(const ColumnData& col, const Value& value);
ColumnData le(const ColumnData& col, const Value& value);
ColumnData eq(const ColumnData& col, const Value& value);
ColumnData ne(const ColumnData& col, const Value& value);

// Cumulative operations
ColumnData cumsum(const ColumnData& col);
ColumnData cumprod(const ColumnData& col);
ColumnData cummin(const ColumnData& col);
ColumnData cummax(const ColumnData& col);

// Matrix operations
DataFrame dot(const DataFrame& df1, const DataFrame& df2);
DataFrame transpose(const DataFrame& df);

// Utility functions
bool is_numeric(const Value& value);
bool is_nan(const Value& value);

// DataFrame arithmetic operators (as regular functions)
DataFrame operator+(const DataFrame& df, double value);
DataFrame operator-(const DataFrame& df, double value);
DataFrame operator*(const DataFrame& df, double value);
DataFrame operator/(const DataFrame& df, double value);

} // namespace math
} // namespace df

#endif // DF_DS_LIBRARY_MATH_H