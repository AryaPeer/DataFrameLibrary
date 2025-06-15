#include "df/stats.hpp"
#include "df/dataframe.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>

namespace df { namespace stats {
Value mean(const DataFrame& df, const std::string& columnName) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
    return mean(df[columnName]);
}

Value sum(const DataFrame& df, const std::string& columnName) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
    return sum(df[columnName]);
}

Value max(const DataFrame& df, const std::string& columnName) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
    return max(df[columnName]);
}

Value min(const DataFrame& df, const std::string& columnName) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
    return min(df[columnName]);
}

Value median(const DataFrame& df, const std::string& columnName) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
    return median(df[columnName]);
}

Value count(const DataFrame& df, const std::string& columnName) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
    return count(df[columnName]);
}

Value var(const DataFrame& df, const std::string& columnName, size_t ddof) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
    return var(df[columnName], ddof);
}

Value std(const DataFrame& df, const std::string& columnName, size_t ddof) {
    Value variance = var(df, columnName, ddof);
    
    if (std::holds_alternative<double>(variance)) {
        double stddev = std::sqrt(std::get<double>(variance));
        return stddev;
    } else if (std::holds_alternative<int>(variance)) {
        double stddev = std::sqrt(static_cast<double>(std::get<int>(variance)));
        return stddev;
    } else {
        return NA_VALUE;
    }
}

DescribeResult describe(const DataFrame& df, const std::string& columnName) {
    DescribeResult result;
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist: " + columnName);
    }
      const auto& colData = df[columnName];
    
    Value countVal = count(colData);
    Value meanVal = mean(colData);
    Value stdVal = std(colData);
    Value minVal = min(colData);
    Value medianVal = median(colData);
    Value maxVal = max(colData);
    
    result.count = std::holds_alternative<int>(countVal) ? std::get<int>(countVal) : 0;
    result.mean = std::holds_alternative<double>(meanVal) ? std::get<double>(meanVal) : 0.0;
    result.std = std::holds_alternative<double>(stdVal) ? std::get<double>(stdVal) : 0.0;
    result.min = std::holds_alternative<double>(minVal) ? std::get<double>(minVal) :
                 (std::holds_alternative<int>(minVal) ? static_cast<double>(std::get<int>(minVal)) : 0.0);
    result.median = std::holds_alternative<double>(medianVal) ? std::get<double>(medianVal) :
                    (std::holds_alternative<int>(medianVal) ? static_cast<double>(std::get<int>(medianVal)) : 0.0);
    result.max = std::holds_alternative<double>(maxVal) ? std::get<double>(maxVal) :
                 (std::holds_alternative<int>(maxVal) ? static_cast<double>(std::get<int>(maxVal)) : 0.0);
    
    std::vector<double> sortedValues;
    std::visit([&](const auto& vec) {
        for (const auto& val : vec) {
            if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<int>>) {
                sortedValues.push_back(static_cast<double>(val));
            } else if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<double>>) {
                sortedValues.push_back(val);
            }
        }
    }, colData);
    
    std::sort(sortedValues.begin(), sortedValues.end());
    
    if (!sortedValues.empty()) {
        size_t q1_idx = sortedValues.size() / 4;
        size_t q3_idx = 3 * sortedValues.size() / 4;
        result.q25 = sortedValues[q1_idx];
        result.q75 = sortedValues[q3_idx];
    } else {
        result.q25 = 0.0;
        result.q75 = 0.0;
    }
    
    return result;
}

std::map<std::string, DescribeResult> describe(const DataFrame& df) {
    std::map<std::string, DescribeResult> results;
      for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            results[colName] = describe(df, colName);
        }
    }
    
    return results;
}

DataFrame corr(const DataFrame& df) {
    std::map<std::string, ColumnData> correlations;
    auto columnNames = df.getColumnNames();
    
    std::vector<std::string> numericColNames;
    for (const auto& colName : columnNames) {
        const auto& colData = df[colName];
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            numericColNames.push_back(colName);
        }
    }
    
    for (const auto& colName : numericColNames) {
        DoubleColumn correlationColumn;
        correlationColumn.reserve(numericColNames.size());
        for (const auto& otherColName : numericColNames) {
            if (colName == otherColName) {
                correlationColumn.push_back(1.0);
            } else {
                const auto& col1 = df[colName];
                const auto& col2 = df[otherColName];
                
                std::vector<double> vals1, vals2;
                std::visit([&](const auto& vec) {
                    for (const auto& val : vec) {
                        if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<int>>) {
                            vals1.push_back(static_cast<double>(val));
                        } else if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<double>>) {
                            vals1.push_back(val);
                        }
                    }
                }, col1);
                
                std::visit([&](const auto& vec) {
                    for (const auto& val : vec) {
                        if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<int>>) {
                            vals2.push_back(static_cast<double>(val));
                        } else if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<double>>) {
                            vals2.push_back(val);
                        }
                    }
                }, col2);
                  if (vals1.size() == vals2.size() && !vals1.empty()) {
                    double mean1 = std::accumulate(vals1.begin(), vals1.end(), 0.0) / vals1.size();
                    double mean2 = std::accumulate(vals2.begin(), vals2.end(), 0.0) / vals2.size();
                    
                    double numerator = 0.0, denom1 = 0.0, denom2 = 0.0;
                    for (size_t i = 0; i < vals1.size(); ++i) {
                        double diff1 = vals1[i] - mean1;
                        double diff2 = vals2[i] - mean2;
                        numerator += diff1 * diff2;
                        denom1 += diff1 * diff1;
                        denom2 += diff2 * diff2;
                    }
                    
                    double correlation = (denom1 * denom2 > 0) ? numerator / std::sqrt(denom1 * denom2) : 0.0;
                    correlationColumn.push_back(correlation);
                } else {
                    correlationColumn.push_back(0.0);
                }
            }
        }
        
        correlations[colName] = correlationColumn;
    }
    
    return DataFrame(correlations);
}

DataFrame cov(const DataFrame& df) {
    std::map<std::string, ColumnData> covariances;
    auto columnNames = df.getColumnNames();
    
    std::vector<std::string> numericColNames;
    for (const auto& colName : columnNames) {
        const auto& colData = df[colName];
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            numericColNames.push_back(colName);
        }
    }
    
    for (const auto& colName : numericColNames) {
        DoubleColumn covarianceColumn;
        covarianceColumn.reserve(numericColNames.size());
          for (const auto& otherColName : numericColNames) {
            if (colName == otherColName) {
                Value variance = var(df, colName);if (std::holds_alternative<double>(variance)) {
                    covarianceColumn.push_back(std::get<double>(variance));
                } else if (std::holds_alternative<int>(variance)) {
                    covarianceColumn.push_back(static_cast<double>(std::get<int>(variance)));} else {                    covarianceColumn.push_back(0.0);
                }
            } else {
                const auto& col1 = df[colName];
                const auto& col2 = df[otherColName];
                
                std::vector<double> vals1, vals2;
                std::visit([&](const auto& vec) {
                    for (const auto& val : vec) {
                        if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<int>>) {
                            vals1.push_back(static_cast<double>(val));
                        } else if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<double>>) {
                            vals1.push_back(val);
                        }
                    }
                }, col1);
                
                std::visit([&](const auto& vec) {
                    for (const auto& val : vec) {
                        if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<int>>) {
                            vals2.push_back(static_cast<double>(val));
                        } else if constexpr (std::is_same_v<std::decay_t<decltype(vec)>, std::vector<double>>) {
                            vals2.push_back(val);
                        }
                    }
                }, col2);
                
                if (vals1.size() == vals2.size() && vals1.size() > 1) {
                    // Calculate means
                    double mean1 = std::accumulate(vals1.begin(), vals1.end(), 0.0) / vals1.size();
                    double mean2 = std::accumulate(vals2.begin(), vals2.end(), 0.0) / vals2.size();
                    
                    // Calculate covariance
                    double covariance = 0.0;
                    for (size_t i = 0; i < vals1.size(); ++i) {
                        covariance += (vals1[i] - mean1) * (vals2[i] - mean2);
                    }
                    covariance /= (vals1.size() - 1);  // Sample covariance
                    covarianceColumn.push_back(covariance);
                } else {
                    covarianceColumn.push_back(0.0);
                }
            }
        }
        
        covariances[colName] = covarianceColumn;
    }
    
    return DataFrame(covariances);
}

// ColumnData functions
Value mean(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;
        
        size_t count = 0;
        double sum = 0.0;
        
        for (const auto& val : vec) {
            if constexpr (std::is_same_v<T, NullableInt> || 
                         std::is_same_v<T, NullableDouble> || 
                         std::is_same_v<T, NullableBool>) {
                if (!val.is_na()) {
                    if constexpr (std::is_same_v<T, NullableInt> || 
                                 std::is_same_v<T, NullableDouble>) {
                        sum += static_cast<double>(val.value_unsafe());
                        count++;
                    } else if constexpr (std::is_same_v<T, NullableBool>) {
                        sum += val.value_unsafe() ? 1.0 : 0.0;
                        count++;
                    }
                }
            }
            else if constexpr (std::is_same_v<T, std::optional<std::string>>) {
                // Skip strings for numerical operations
                continue;
            }
        }
        
        if (count == 0) {
            return NA_VALUE;
        }
        return sum / count;
    }, column);
}

Value sum(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;
        
        if constexpr (std::is_same_v<T, NullableInt>) {
            int sum = 0;
            bool any_valid = false;
            
            for (const auto& val : vec) {
                if (!val.is_na()) {
                    sum += val.value_unsafe();
                    any_valid = true;
                }
            }
            
            if (!any_valid) {
                return NA_VALUE;
            }
            return sum;
        } 
        else if constexpr (std::is_same_v<T, NullableDouble>) {
            double sum = 0.0;
            bool any_valid = false;
            
            for (const auto& val : vec) {
                if (!val.is_na()) {
                    sum += val.value_unsafe();
                    any_valid = true;
                }
            }
            
            if (!any_valid) {
                return NA_VALUE;
            }
            return sum;
        } 
        else if constexpr (std::is_same_v<T, NullableBool>) {
            int sum = 0;
            bool any_valid = false;
            
            for (const auto& val : vec) {
                if (!val.is_na()) {
                    sum += val.value_unsafe() ? 1 : 0;
                    any_valid = true;
                }
            }
            
            if (!any_valid) {
                return NA_VALUE;
            }
            return sum;
        } 
        else if constexpr (std::is_same_v<T, std::optional<std::string>>) {
            // Cannot sum strings
            return NA_VALUE;
        }
        else {
            return NA_VALUE;
        }
    }, column);
}

Value max(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;
        
        if (vec.empty()) {
            return NA_VALUE;
        }
        
        if constexpr (std::is_same_v<T, NullableInt> || 
                     std::is_same_v<T, NullableDouble> || 
                     std::is_same_v<T, NullableBool>) {
            auto it = std::max_element(vec.begin(), vec.end(), 
                [](const auto& a, const auto& b) {
                    if (a.is_na()) return true;
                    if (b.is_na()) return false;
                    return a.value_unsafe() < b.value_unsafe();
                });
                
            if (it == vec.end() || it->is_na()) {
                return NA_VALUE;
            }
            
            return it->value_unsafe();
        }
        else if constexpr (std::is_same_v<T, std::optional<std::string>>) {
            std::optional<std::string> max_val;
            bool any_valid = false;
            
            for (const auto& val : vec) {
                if (val.has_value()) {
                    if (!any_valid || (!max_val.has_value() || val.value() > max_val.value())) {
                        max_val = val;
                        any_valid = true;
                    }
                }
            }
            
            if (!any_valid || !max_val.has_value()) {
                return NA_VALUE;
            }
            
            return max_val.value();
        } 
        else {
            return NA_VALUE;
        }
    }, column);
}

Value min(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;
        
        if (vec.empty()) {
            return NA_VALUE;
        }
        
        if constexpr (std::is_same_v<T, NullableInt> || 
                     std::is_same_v<T, NullableDouble> || 
                     std::is_same_v<T, NullableBool>) {
            auto it = std::min_element(vec.begin(), vec.end(), 
                [](const auto& a, const auto& b) {
                    if (a.is_na()) return false;
                    if (b.is_na()) return true;
                    return a.value_unsafe() < b.value_unsafe();
                });
                
            if (it == vec.end() || it->is_na()) {
                return NA_VALUE;
            }
            
            return it->value_unsafe();
        }
        else if constexpr (std::is_same_v<T, std::optional<std::string>>) {
            std::optional<std::string> min_val;
            bool any_valid = false;
            
            for (const auto& val : vec) {
                if (val.has_value()) {
                    if (!any_valid || (!min_val.has_value() || val.value() < min_val.value())) {
                        min_val = val;
                        any_valid = true;
                    }
                }
            }
            
            if (!any_valid || !min_val.has_value()) {
                return NA_VALUE;
            }
            
            return min_val.value();
        } 
        else {
            return NA_VALUE;
        }
    }, column);
}

Value median(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;
        
        if constexpr (std::is_same_v<T, NullableInt> || std::is_same_v<T, NullableDouble>) {
            // First, collect non-NA values
            std::vector<double> values;
            for (const auto& val : vec) {
                if (!val.is_na()) {
                    values.push_back(static_cast<double>(val.value_unsafe()));
                }
            }
            
            if (values.empty()) {
                return NA_VALUE;
            }
            
            std::sort(values.begin(), values.end());
            
            size_t size = values.size();
            if (size % 2 == 0) {
                // Even number of elements - take average of middle two
                return (values[size / 2 - 1] + values[size / 2]) / 2.0;
            } else {
                // Odd number of elements - take middle element
                return values[size / 2];
            }
        }
        else if constexpr (std::is_same_v<T, std::optional<std::string>>) {
            // No median for strings
            return NA_VALUE;
        }
        else {
            return NA_VALUE;
        }
    }, column);
}

Value count(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;
        
        int validCount = 0;
        
        for (const auto& val : vec) {
            if constexpr (std::is_same_v<T, NullableInt> || 
                         std::is_same_v<T, NullableDouble> ||
                         std::is_same_v<T, NullableBool>) {
                if (!val.is_na()) {
                    validCount++;
                }
            }
            else if constexpr (std::is_same_v<T, std::optional<std::string>>) {
                if (val.has_value()) {
                    validCount++;
                }
            }
        }
        
        return validCount;
    }, column);
}

Value var(const ColumnData& column, size_t ddof) {
    return std::visit([ddof](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;
        
        if constexpr (std::is_same_v<T, NullableInt> || std::is_same_v<T, NullableDouble>) {
            double mean_val = 0.0; // Renamed from mean to avoid conflict if a 'mean' variable exists in outer scope
            double sum_squared_diff = 0.0;
            size_t count = 0;
            
            for (const auto& val : vec) {
                if (!val.is_na()) {
                    mean_val += static_cast<double>(val.value_unsafe());
                    count++;
                }
            }
            
            if (count <= ddof) {
                return NA_VALUE;
            }
            
            mean_val /= count;
            
            for (const auto& val : vec) {
                if (!val.is_na()) {
                    double diff = static_cast<double>(val.value_unsafe()) - mean_val;
                    sum_squared_diff += diff * diff;
                }
            }
            
            return sum_squared_diff / (count - ddof);
        }
        else if constexpr (std::is_same_v<T, std::optional<std::string>>) {
            // Cannot calculate variance for strings
            return NA_VALUE;
        } 
        else {
            return NA_VALUE;
        }
    }, column);
}

Value std(const ColumnData& column, size_t ddof) {
    Value variance = var(column, ddof);
    
    if (std::holds_alternative<double>(variance)) {
        double stddev = std::sqrt(std::get<double>(variance));
        return stddev;
    } else if (std::holds_alternative<int>(variance)) {
        double stddev = std::sqrt(static_cast<double>(std::get<int>(variance)));
        return stddev;
    } else {
        return NA_VALUE;
    }
}


} }
