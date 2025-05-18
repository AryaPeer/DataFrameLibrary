#ifndef DF_DS_LIBRARY_MATH_H
#define DF_DS_LIBRARY_MATH_H

#include "df/types.hpp"

namespace df {

class DataFrame;

namespace math {

void addInPlace(DataFrame& df, const std::string& columnName, const Value& value);
void subtractInPlace(DataFrame& df, const std::string& columnName, const Value& value);
void multiplyInPlace(DataFrame& df, const std::string& columnName, const Value& value);
void divideInPlace(DataFrame& df, const std::string& columnName, const Value& value);

DataFrame add(const DataFrame& df, const DataFrame& other, const Value& fillValue = NA_VALUE);
DataFrame subtract(const DataFrame& df, const DataFrame& other, const Value& fillValue = NA_VALUE);
DataFrame multiply(const DataFrame& df, const DataFrame& other, const Value& fillValue = NA_VALUE);
DataFrame divide(const DataFrame& df, const DataFrame& other, const Value& fillValue = NA_VALUE);

DataFrame add(const DataFrame& df, const Value& value);
DataFrame subtract(const DataFrame& df, const Value& value);
DataFrame multiply(const DataFrame& df, const Value& value);
DataFrame divide(const DataFrame& df, const Value& value);

} // namespace math
} // namespace df

#endif // DF_DS_LIBRARY_MATH_H
