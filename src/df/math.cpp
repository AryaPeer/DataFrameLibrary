#include "df/math.hpp"
#include "df/dataframe.hpp"
#include <stdexcept>
#include <type_traits>

namespace df {
namespace math {

void add_inplace(DataFrame& df, const std::string& columnName, const Value& value) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    
    auto& colData = df[columnName];
    if (std::holds_alternative<double>(value)) {
        double val = std::get<double>(value);
        std::visit(
            [val](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                using ValueType = typename VecType::value_type;
                if constexpr (std::is_same_v<ValueType, NullableInt> || 
                              std::is_same_v<ValueType, NullableDouble>) {
                    for (auto& elem : vec) {
                        if (!elem.is_na()) {
                            elem += val;
                        }
                    }
                }
                // Skip non-numeric columns silently
            },
            colData);
    } else if (std::holds_alternative<int>(value)) {
        int val = std::get<int>(value);
        std::visit(
            [val](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                using ValueType = typename VecType::value_type;
                if constexpr (std::is_same_v<ValueType, NullableInt> || 
                              std::is_same_v<ValueType, NullableDouble>) {
                    for (auto& elem : vec) {
                        if (!elem.is_na()) {
                            elem += val;
                        }
                    }
                }
                // Skip non-numeric columns silently
            },
            colData);
    } else {
        throw std::invalid_argument("Unsupported value type for arithmetic operation.");
    }
}

void subtract_inplace(DataFrame& df, const std::string& columnName, const Value& value) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    
    auto& colData = df[columnName];
    // Handle based on value type and column type
    if (std::holds_alternative<double>(value)) {
        double val = std::get<double>(value);
        std::visit(
            [val](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                using ValueType = typename VecType::value_type;
                if constexpr (std::is_same_v<ValueType, NullableInt> || 
                              std::is_same_v<ValueType, NullableDouble>) {
                    for (auto& elem : vec) {
                        if (!elem.is_na()) {
                            elem -= val;
                        }
                    }
                }
                // Skip non-numeric columns silently
            },
            colData);
    } else if (std::holds_alternative<int>(value)) {
        int val = std::get<int>(value);
        std::visit(
            [val](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                using ValueType = typename VecType::value_type;
                if constexpr (std::is_same_v<ValueType, NullableInt> || 
                              std::is_same_v<ValueType, NullableDouble>) {
                    for (auto& elem : vec) {
                        if (!elem.is_na()) {
                            elem -= val;
                        }
                    }
                }
                // Skip non-numeric columns silently
            },
            colData);
    } else {
        throw std::invalid_argument("Unsupported value type for arithmetic operation.");
    }
}

void multiply_inplace(DataFrame& df, const std::string& columnName, const Value& value) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    
    auto& colData = df[columnName];
    // Handle based on value type and column type
    if (std::holds_alternative<double>(value)) {
        double val = std::get<double>(value);
        std::visit(
            [val](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                using ValueType = typename VecType::value_type;
                if constexpr (std::is_same_v<ValueType, NullableInt> || 
                              std::is_same_v<ValueType, NullableDouble>) {
                    for (auto& elem : vec) {
                        if (!elem.is_na()) {
                            elem *= val;
                        }
                    }
                }
                // Skip non-numeric columns silently
            },
            colData);
    } else if (std::holds_alternative<int>(value)) {
        int val = std::get<int>(value);
        std::visit(
            [val](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                using ValueType = typename VecType::value_type;
                if constexpr (std::is_same_v<ValueType, NullableInt> || 
                              std::is_same_v<ValueType, NullableDouble>) {
                    for (auto& elem : vec) {
                        if (!elem.is_na()) {
                            elem *= val;
                        }
                    }
                }
                // Skip non-numeric columns silently
            },
            colData);
    } else {
        throw std::invalid_argument("Unsupported value type for arithmetic operation.");
    }
}

void divide_inplace(DataFrame& df, const std::string& columnName, const Value& value) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    
    // Check for division by zero
    if ((std::holds_alternative<double>(value) && std::get<double>(value) == 0) ||
        (std::holds_alternative<int>(value) && std::get<int>(value) == 0)) {
        throw std::invalid_argument("Division by zero.");
    }
    
    auto& colData = df[columnName];
    // Handle based on value type and column type
    if (std::holds_alternative<double>(value)) {
        double val = std::get<double>(value);
        std::visit(
            [val](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                using ValueType = typename VecType::value_type;
                if constexpr (std::is_same_v<ValueType, NullableInt> || 
                              std::is_same_v<ValueType, NullableDouble>) {
                    for (auto& elem : vec) {
                        if (!elem.is_na()) {
                            elem /= val;
                        }
                    }
                }
                // Skip non-numeric columns silently
            },
            colData);
    } else if (std::holds_alternative<int>(value)) {
        int val = std::get<int>(value);
        std::visit(
            [val](auto&& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                using ValueType = typename VecType::value_type;
                if constexpr (std::is_same_v<ValueType, NullableInt> || 
                              std::is_same_v<ValueType, NullableDouble>) {
                    for (auto& elem : vec) {
                        if (!elem.is_na()) {
                            elem /= val;
                        }
                    }
                }
                // Skip non-numeric columns silently
            },
            colData);
    } else {
        throw std::invalid_argument("Unsupported value type for arithmetic operation.");
    }
}

DataFrame add(const DataFrame& df, const DataFrame& other, const Value& fill_value) {
    DataFrame result = df;
    
    for (const auto& [colName, colData] : other.getColumns()) {
        if (df.columnExists(colName)) {
            // Column exists in both DataFrames, we need to add values
            auto resultCol = df[colName]; // Getting a copy of the column
            auto otherCol = other[colName];
            
            if (std::holds_alternative<IntColumn>(resultCol) && std::holds_alternative<IntColumn>(otherCol)) {
                auto& resultVec = std::get<IntColumn>(resultCol);
                const auto& otherVec = std::get<IntColumn>(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                
                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() + otherVec[i].value_unsafe();
                    } else if (!std::holds_alternative<NA>(fill_value)) {
                        // Use fill_value if one value is NA
                        if (std::holds_alternative<int>(fill_value)) {
                            int fillVal = std::get<int>(fill_value);
                            if (resultVec[i].is_na()) {
                                resultVec[i] = (otherVec[i].is_na()) ? fillVal : otherVec[i].value_unsafe();
                            } else {
                                resultVec[i] = resultVec[i].value_unsafe() + (otherVec[i].is_na() ? fillVal : otherVec[i].value_unsafe());
                            }
                        }
                    }
                }
                
                result.removeColumn(colName);
                result.addColumn(colName, resultCol);
            } else if (std::holds_alternative<DoubleColumn>(resultCol) && std::holds_alternative<DoubleColumn>(otherCol)) {
                auto& resultVec = std::get<DoubleColumn>(resultCol);
                const auto& otherVec = std::get<DoubleColumn>(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                
                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() + otherVec[i].value_unsafe();
                    } else if (!std::holds_alternative<NA>(fill_value)) {
                        if (std::holds_alternative<double>(fill_value)) {
                            double fillVal = std::get<double>(fill_value);
                            if (resultVec[i].is_na()) {
                                resultVec[i] = (otherVec[i].is_na()) ? fillVal : otherVec[i].value_unsafe();
                            } else {
                                resultVec[i] = resultVec[i].value_unsafe() + (otherVec[i].is_na() ? fillVal : otherVec[i].value_unsafe());
                            }
                        }
                    }
                }
                
                result.removeColumn(colName);
                result.addColumn(colName, resultCol);
            }
        } else {
            result.addColumn(colName, colData);
        }
    }
    
    return result;
}

DataFrame subtract(const DataFrame& df, const DataFrame& other, const Value& fill_value) {
    DataFrame result = df;
    
    for (const auto& [colName, colData] : other.getColumns()) {
        if (df.columnExists(colName)) {
            // Column exists in both DataFrames, we need to subtract values
            auto resultCol = df[colName]; // Getting a copy of the column
            auto otherCol = other[colName];
            
            // Subtract column values
            if (std::holds_alternative<IntColumn>(resultCol) && std::holds_alternative<IntColumn>(otherCol)) {
                auto& resultVec = std::get<IntColumn>(resultCol);
                const auto& otherVec = std::get<IntColumn>(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                
                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() - otherVec[i].value_unsafe();
                    } else if (!std::holds_alternative<NA>(fill_value)) {
                        // Use fill_value if one value is NA
                        if (std::holds_alternative<int>(fill_value)) {
                            int fillVal = std::get<int>(fill_value);
                            if (resultVec[i].is_na()) {
                                resultVec[i] = (otherVec[i].is_na()) ? fillVal : -otherVec[i].value_unsafe();
                            } else {
                                resultVec[i] = resultVec[i].value_unsafe() - (otherVec[i].is_na() ? fillVal : otherVec[i].value_unsafe());
                            }
                        }
                    }
                }
                
                result.removeColumn(colName);
                result.addColumn(colName, resultCol);
            } else if (std::holds_alternative<DoubleColumn>(resultCol) && std::holds_alternative<DoubleColumn>(otherCol)) {
                auto& resultVec = std::get<DoubleColumn>(resultCol);
                const auto& otherVec = std::get<DoubleColumn>(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                
                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() - otherVec[i].value_unsafe();
                    } else if (!std::holds_alternative<NA>(fill_value)) {
                        // Use fill_value if one value is NA
                        if (std::holds_alternative<double>(fill_value)) {
                            double fillVal = std::get<double>(fill_value);
                            if (resultVec[i].is_na()) {
                                resultVec[i] = (otherVec[i].is_na()) ? fillVal : -otherVec[i].value_unsafe();
                            } else {
                                resultVec[i] = resultVec[i].value_unsafe() - (otherVec[i].is_na() ? fillVal : otherVec[i].value_unsafe());
                            }
                        }
                    }
                }
                
                result.removeColumn(colName);
                result.addColumn(colName, resultCol);
            }
        }
    }
    
    return result;
}

DataFrame multiply(const DataFrame& df, const DataFrame& other, const Value& fill_value) {
    DataFrame result = df;
    
    for (const auto& [colName, colData] : other.getColumns()) {
        if (df.columnExists(colName)) {
            // Column exists in both DataFrames, we need to multiply values
            auto resultCol = df[colName]; // Getting a copy of the column
            auto otherCol = other[colName];
            
            // Multiply column values
            if (std::holds_alternative<IntColumn>(resultCol) && std::holds_alternative<IntColumn>(otherCol)) {
                auto& resultVec = std::get<IntColumn>(resultCol);
                const auto& otherVec = std::get<IntColumn>(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                
                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() * otherVec[i].value_unsafe();
                    } else if (!std::holds_alternative<NA>(fill_value)) {
                        // Use fill_value if one value is NA
                        if (std::holds_alternative<int>(fill_value)) {
                            int fillVal = std::get<int>(fill_value);
                            if (resultVec[i].is_na()) {
                                resultVec[i] = (otherVec[i].is_na()) ? fillVal : otherVec[i].value_unsafe() * fillVal;
                            } else {
                                resultVec[i] = resultVec[i].value_unsafe() * (otherVec[i].is_na() ? fillVal : otherVec[i].value_unsafe());
                            }
                        }
                    } else {
                        resultVec[i] = NA_VALUE; // Result is NA if either input is NA
                    }
                }
                
                result.removeColumn(colName);
                result.addColumn(colName, resultCol);
            } else if (std::holds_alternative<DoubleColumn>(resultCol) && std::holds_alternative<DoubleColumn>(otherCol)) {
                auto& resultVec = std::get<DoubleColumn>(resultCol);
                const auto& otherVec = std::get<DoubleColumn>(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                
                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() * otherVec[i].value_unsafe();
                    } else if (!std::holds_alternative<NA>(fill_value)) {
                        // Use fill_value if one value is NA
                        if (std::holds_alternative<double>(fill_value)) {
                            double fillVal = std::get<double>(fill_value);
                            if (resultVec[i].is_na()) {
                                resultVec[i] = (otherVec[i].is_na()) ? fillVal : otherVec[i].value_unsafe() * fillVal;
                            } else {
                                resultVec[i] = resultVec[i].value_unsafe() * (otherVec[i].is_na() ? fillVal : otherVec[i].value_unsafe());
                            }
                        }
                    } else {
                        resultVec[i] = NA_VALUE; // Result is NA if either input is NA
                    }
                }
                
                result.removeColumn(colName);
                result.addColumn(colName, resultCol);
            }
        }
    }
    
    return result;
}

DataFrame divide(const DataFrame& df, const DataFrame& other, const Value& fill_value) {
    DataFrame result = df;
    
    for (const auto& [colName, colData] : other.getColumns()) {
        if (df.columnExists(colName)) {
            // Column exists in both DataFrames, we need to divide values
            auto resultCol = df[colName]; // Getting a copy of the column
            auto otherCol = other[colName];
            
            // Divide column values
            if (std::holds_alternative<IntColumn>(resultCol) && std::holds_alternative<IntColumn>(otherCol)) {
                auto& resultVec = std::get<IntColumn>(resultCol);
                const auto& otherVec = std::get<IntColumn>(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                
                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na() && otherVec[i].value_unsafe() != 0) {
                        resultVec[i] = resultVec[i].value_unsafe() / otherVec[i].value_unsafe();
                    } else if (!std::holds_alternative<NA>(fill_value)) {
                        // Use fill_value if one value is NA or divisor is zero
                        if (std::holds_alternative<int>(fill_value)) {
                            int fillVal = std::get<int>(fill_value);
                            if (resultVec[i].is_na()) {
                                resultVec[i] = fillVal;
                            } else if (otherVec[i].is_na() || otherVec[i].value_unsafe() == 0) {
                                resultVec[i] = fillVal;
                            }
                        }
                    } else {
                        resultVec[i] = NA_VALUE; // Result is NA if either input is NA or divisor is zero
                    }
                }
                
                result.removeColumn(colName);
                result.addColumn(colName, resultCol);
            } else if (std::holds_alternative<DoubleColumn>(resultCol) && std::holds_alternative<DoubleColumn>(otherCol)) {
                auto& resultVec = std::get<DoubleColumn>(resultCol);
                const auto& otherVec = std::get<DoubleColumn>(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                
                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na() && otherVec[i].value_unsafe() != 0) {
                        resultVec[i] = resultVec[i].value_unsafe() / otherVec[i].value_unsafe();
                    } else if (!std::holds_alternative<NA>(fill_value)) {
                        // Use fill_value if one value is NA or divisor is zero
                        if (std::holds_alternative<double>(fill_value)) {
                            double fillVal = std::get<double>(fill_value);
                            if (resultVec[i].is_na()) {
                                resultVec[i] = fillVal;
                            } else if (otherVec[i].is_na() || otherVec[i].value_unsafe() == 0) {
                                resultVec[i] = fillVal;
                            }
                        }
                    } else {
                        resultVec[i] = NA_VALUE; // Result is NA if either input is NA or divisor is zero
                    }
                }
                
                result.removeColumn(colName);
                result.addColumn(colName, resultCol);
            }
        }
    }
    
    return result;
}

// DataFrame-scalar operations
DataFrame add(const DataFrame& df, const Value& value) {
    DataFrame result = df;
    
    for (const auto& [colName, _] : df.getColumns()) {
        add_inplace(result, colName, value);
    }
    
    return result;
}

DataFrame subtract(const DataFrame& df, const Value& value) {
    DataFrame result = df;
    
    for (const auto& [colName, _] : df.getColumns()) {
        subtract_inplace(result, colName, value);
    }
    
    return result;
}

DataFrame multiply(const DataFrame& df, const Value& value) {
    DataFrame result = df;
    
    for (const auto& [colName, _] : df.getColumns()) {
        multiply_inplace(result, colName, value);
    }
    
    return result;
}

DataFrame divide(const DataFrame& df, const Value& value) {
    // Check for division by zero for scalar values
    if ((std::holds_alternative<double>(value) && std::get<double>(value) == 0) ||
        (std::holds_alternative<int>(value) && std::get<int>(value) == 0)) {
        throw std::invalid_argument("Division by zero.");
    }
    
    DataFrame result = df;
    
    for (const auto& [colName, _] : df.getColumns()) {
        divide_inplace(result, colName, value);
    }
    
    return result;
}

DataFrame operator+(const DataFrame& df, double value) {
    DataFrame result = df;
    for (size_t i = 0; i < df.numColumns(); ++i) {
        std::string colName = result.getColumnName(i);
        add_inplace(result, colName, value);
    }
    return result;
}

DataFrame operator-(const DataFrame& df, double value) {
    DataFrame result = df;
    for (size_t i = 0; i < df.numColumns(); ++i) {
        std::string colName = result.getColumnName(i);
        subtract_inplace(result, colName, value);
    }
    return result;
}

DataFrame operator*(const DataFrame& df, double value) {
    DataFrame result = df;
    for (size_t i = 0; i < df.numColumns(); ++i) {
        std::string colName = result.getColumnName(i);
        multiply_inplace(result, colName, value);
    }
    return result;
}

DataFrame operator/(const DataFrame& df, double value) {
    if (value == 0) {
        throw std::invalid_argument("Division by zero.");
    }
    
    DataFrame result = df;
    for (size_t i = 0; i < df.numColumns(); ++i) {
        std::string colName = result.getColumnName(i);
        divide_inplace(result, colName, value);
    }
    return result;
}

} // namespace math
} // namespace df