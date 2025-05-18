#ifndef DF_DS_LIBRARY_MATH_H
#define DF_DS_LIBRARY_MATH_H

#include "df/types.hpp"
#include <cmath>

namespace df {

// Forward declarations
class DataFrame;

namespace math {

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

// DataFrame arithmetic operators (as regular functions)
DataFrame operator+(const DataFrame& df, double value);
DataFrame operator-(const DataFrame& df, double value);
DataFrame operator*(const DataFrame& df, double value);
DataFrame operator/(const DataFrame& df, double value);

} // namespace math
} // namespace df

#endif // DF_DS_LIBRARY_MATH_H