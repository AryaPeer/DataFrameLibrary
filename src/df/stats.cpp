#include "df/stats.hpp"
#include "df/dataframe.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>
#include <limits>

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
    
    constexpr double NaN = std::numeric_limits<double>::quiet_NaN();
    result.count = std::holds_alternative<int>(countVal) ? std::get<int>(countVal) : 0;
    result.mean = std::holds_alternative<double>(meanVal) ? std::get<double>(meanVal) : NaN;
    result.std = std::holds_alternative<double>(stdVal) ? std::get<double>(stdVal) : NaN;
    result.min = std::holds_alternative<double>(minVal) ? std::get<double>(minVal) :
                 (std::holds_alternative<int>(minVal) ? static_cast<double>(std::get<int>(minVal)) : NaN);
    result.median = std::holds_alternative<double>(medianVal) ? std::get<double>(medianVal) :
                    (std::holds_alternative<int>(medianVal) ? static_cast<double>(std::get<int>(medianVal)) : NaN);
    result.max = std::holds_alternative<double>(maxVal) ? std::get<double>(maxVal) :
                 (std::holds_alternative<int>(maxVal) ? static_cast<double>(std::get<int>(maxVal)) : NaN);
    
    std::vector<double> sortedValues;
    std::visit([&](const auto& vec) {
        using VecType = std::decay_t<decltype(vec)>;
        if constexpr (std::is_same_v<VecType, IntColumn>) {
            for (const auto& val : vec) {
                if (!val.is_na()) sortedValues.push_back(static_cast<double>(val.value_unsafe()));
            }
        } else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
            for (const auto& val : vec) {
                if (!val.is_na()) sortedValues.push_back(val.value_unsafe());
            }
        }
    }, colData);

    std::sort(sortedValues.begin(), sortedValues.end());

    if (!sortedValues.empty()) {
        auto interpolate = [&](double q) -> double {
            double pos = q * (sortedValues.size() - 1);
            size_t lower = static_cast<size_t>(pos);
            size_t upper = lower + 1;
            double frac = pos - lower;
            if (upper >= sortedValues.size()) return sortedValues[lower];
            return sortedValues[lower] * (1.0 - frac) + sortedValues[upper] * frac;
        };
        result.q25 = interpolate(0.25);
        result.q75 = interpolate(0.75);
    } else {
        result.q25 = NaN;
        result.q75 = NaN;
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
    std::vector<std::pair<std::string, ColumnData>> correlations;
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

                // Extract paired non-NA values by index
                size_t colSize = std::visit([](const auto& v) { return v.size(); }, col1);
                std::vector<double> vals1, vals2;

                for (size_t idx = 0; idx < colSize; ++idx) {
                    double v1 = 0.0, v2 = 0.0;
                    bool na1 = false, na2 = false;

                    std::visit([&](const auto& vec) {
                        using VecType = std::decay_t<decltype(vec)>;
                        if constexpr (std::is_same_v<VecType, IntColumn>) {
                            if (vec[idx].is_na()) na1 = true;
                            else v1 = static_cast<double>(vec[idx].value_unsafe());
                        } else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
                            if (vec[idx].is_na()) na1 = true;
                            else v1 = vec[idx].value_unsafe();
                        } else { na1 = true; }
                    }, col1);

                    std::visit([&](const auto& vec) {
                        using VecType = std::decay_t<decltype(vec)>;
                        if constexpr (std::is_same_v<VecType, IntColumn>) {
                            if (vec[idx].is_na()) na2 = true;
                            else v2 = static_cast<double>(vec[idx].value_unsafe());
                        } else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
                            if (vec[idx].is_na()) na2 = true;
                            else v2 = vec[idx].value_unsafe();
                        } else { na2 = true; }
                    }, col2);

                    if (!na1 && !na2) {
                        vals1.push_back(v1);
                        vals2.push_back(v2);
                    }
                }

                if (vals1.size() > 1) {
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

                    double correlation = (denom1 * denom2 > 0)
                        ? numerator / std::sqrt(denom1 * denom2)
                        : std::numeric_limits<double>::quiet_NaN();
                    correlationColumn.push_back(correlation);
                } else {
                    correlationColumn.push_back(NullableDouble(std::numeric_limits<double>::quiet_NaN()));
                }
            }
        }
        
        correlations.emplace_back(colName, correlationColumn);
    }

    DataFrame result(correlations);
    result.setIndex(numericColNames);
    return result;
}

DataFrame cov(const DataFrame& df) {
    std::vector<std::pair<std::string, ColumnData>> covariances;
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
                Value variance = var(df, colName);
                if (std::holds_alternative<double>(variance)) {
                    covarianceColumn.push_back(std::get<double>(variance));
                } else if (std::holds_alternative<int>(variance)) {
                    covarianceColumn.push_back(static_cast<double>(std::get<int>(variance)));
                } else {
                    covarianceColumn.push_back(std::numeric_limits<double>::quiet_NaN());
                }
            } else {
                const auto& col1 = df[colName];
                const auto& col2 = df[otherColName];

                size_t colSize = std::visit([](const auto& v) { return v.size(); }, col1);
                std::vector<double> vals1, vals2;

                for (size_t idx = 0; idx < colSize; ++idx) {
                    double v1 = 0.0, v2 = 0.0;
                    bool na1 = false, na2 = false;

                    std::visit([&](const auto& vec) {
                        using VecType = std::decay_t<decltype(vec)>;
                        if constexpr (std::is_same_v<VecType, IntColumn>) {
                            if (vec[idx].is_na()) na1 = true;
                            else v1 = static_cast<double>(vec[idx].value_unsafe());
                        } else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
                            if (vec[idx].is_na()) na1 = true;
                            else v1 = vec[idx].value_unsafe();
                        } else { na1 = true; }
                    }, col1);

                    std::visit([&](const auto& vec) {
                        using VecType = std::decay_t<decltype(vec)>;
                        if constexpr (std::is_same_v<VecType, IntColumn>) {
                            if (vec[idx].is_na()) na2 = true;
                            else v2 = static_cast<double>(vec[idx].value_unsafe());
                        } else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
                            if (vec[idx].is_na()) na2 = true;
                            else v2 = vec[idx].value_unsafe();
                        } else { na2 = true; }
                    }, col2);

                    if (!na1 && !na2) {
                        vals1.push_back(v1);
                        vals2.push_back(v2);
                    }
                }

                if (vals1.size() > 1) {
                    double mean1 = std::accumulate(vals1.begin(), vals1.end(), 0.0) / vals1.size();
                    double mean2 = std::accumulate(vals2.begin(), vals2.end(), 0.0) / vals2.size();

                    double covariance = 0.0;
                    for (size_t i = 0; i < vals1.size(); ++i) {
                        covariance += (vals1[i] - mean1) * (vals2[i] - mean2);
                    }
                    covariance /= (vals1.size() - 1);
                    covarianceColumn.push_back(covariance);
                } else {
                    covarianceColumn.push_back(NullableDouble(std::numeric_limits<double>::quiet_NaN()));
                }
            }
        }
        
        covariances.emplace_back(colName, covarianceColumn);
    }

    DataFrame result(covariances);
    result.setIndex(numericColNames);
    return result;
}

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
            else if constexpr (std::is_same_v<T, NullableString>) {
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
            long long sum = 0;
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
            if (sum >= std::numeric_limits<int>::min() && sum <= std::numeric_limits<int>::max()) {
                return static_cast<int>(sum);
            }
            return static_cast<double>(sum);
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
        else if constexpr (std::is_same_v<T, NullableString>) {
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
        else if constexpr (std::is_same_v<T, NullableString>) {
            NullableString max_val;
            bool any_valid = false;

            for (const auto& val : vec) {
                if (!val.is_na()) {
                    if (!any_valid || (max_val.is_na() || val.value_unsafe() > max_val.value_unsafe())) {
                        max_val = val;
                        any_valid = true;
                    }
                }
            }

            if (!any_valid || max_val.is_na()) {
                return NA_VALUE;
            }

            return max_val.value_unsafe();
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
        else if constexpr (std::is_same_v<T, NullableString>) {
            NullableString min_val;
            bool any_valid = false;

            for (const auto& val : vec) {
                if (!val.is_na()) {
                    if (!any_valid || (min_val.is_na() || val.value_unsafe() < min_val.value_unsafe())) {
                        min_val = val;
                        any_valid = true;
                    }
                }
            }

            if (!any_valid || min_val.is_na()) {
                return NA_VALUE;
            }

            return min_val.value_unsafe();
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
                return (values[size / 2 - 1] + values[size / 2]) / 2.0;
            } else {
                return values[size / 2];
            }
        }
        else if constexpr (std::is_same_v<T, NullableString>) {
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

        size_t validCount = 0;

        for (const auto& val : vec) {
            if constexpr (std::is_same_v<T, NullableInt> ||
                         std::is_same_v<T, NullableDouble> ||
                         std::is_same_v<T, NullableBool>) {
                if (!val.is_na()) {
                    validCount++;
                }
            }
            else if constexpr (std::is_same_v<T, NullableString>) {
                if (!val.is_na()) {
                    validCount++;
                }
            }
        }

        if (validCount <= static_cast<size_t>(std::numeric_limits<int>::max())) {
            return static_cast<int>(validCount);
        }
        return static_cast<double>(validCount);
    }, column);
}

Value var(const ColumnData& column, size_t ddof) {
    return std::visit([ddof](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;
        
        if constexpr (std::is_same_v<T, NullableInt> || std::is_same_v<T, NullableDouble>) {
            double mean_val = 0.0;
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
        else if constexpr (std::is_same_v<T, NullableString>) {
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
