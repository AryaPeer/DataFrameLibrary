#ifndef DF_DS_LIBRARY_STATS_H
#define DF_DS_LIBRARY_STATS_H

#include "df/types.hpp"
#include <string>
#include <map>
#include <vector>
#include <cmath>

namespace df {

// Forward declarations
class DataFrame;

namespace stats {

// Statistical summaries structure
struct DescribeResult {
    double count;
    double mean;
    double std;
    double min;
    double q25;
    double median;
    double q75;
    double max;
};

// Basic statistics functions
Value mean(const DataFrame& df, const std::string& columnName);
Value sum(const DataFrame& df, const std::string& columnName);
Value max(const DataFrame& df, const std::string& columnName);
Value min(const DataFrame& df, const std::string& columnName);
Value median(const DataFrame& df, const std::string& columnName);
Value var(const DataFrame& df, const std::string& columnName, size_t ddof = 1);
Value std(const DataFrame& df, const std::string& columnName, size_t ddof = 1);
Value count(const DataFrame& df, const std::string& columnName);
Value quantile(const DataFrame& df, const std::string& columnName, double q);

// Column-based statistics
Value mean(const ColumnData& column);
Value sum(const ColumnData& column);
Value max(const ColumnData& column);
Value min(const ColumnData& column);
Value median(const ColumnData& column);
Value var(const ColumnData& column, size_t ddof = 1);
Value std(const ColumnData& column, size_t ddof = 1);
Value count(const ColumnData& column);
Value quantile(const ColumnData& column, double q);

// Statistical summaries
std::map<std::string, DescribeResult> describe(const DataFrame& df);
DescribeResult describe(const DataFrame& df, const std::string& columnName);
std::map<std::string, Value> describe(const ColumnData& column);

// Advanced statistics
DataFrame corr(const DataFrame& df);
DataFrame cov(const DataFrame& df);
ColumnData kurt(const DataFrame& df);
ColumnData skew(const DataFrame& df);
std::map<Value, size_t> value_counts(const ColumnData& column, bool normalize = false, bool sort = true);
std::map<double, size_t> histogram(const ColumnData& column, size_t bins = 10);

// Moving window statistics
ColumnData rolling_mean(const ColumnData& column, size_t window);
ColumnData rolling_sum(const ColumnData& column, size_t window);
ColumnData rolling_std(const ColumnData& column, size_t window);
ColumnData rolling_var(const ColumnData& column, size_t window);
ColumnData rolling_min(const ColumnData& column, size_t window);
ColumnData rolling_max(const ColumnData& column, size_t window);

// Utility functions
template<typename T>
std::vector<T> linspace(T start, T stop, size_t num = 50);

template<typename T>
std::vector<T> arange(T start, T stop, T step = 1);

// Template implementations
template<typename T>
std::vector<T> linspace(T start, T stop, size_t num) {
    if (num < 2) return {start};
    
    std::vector<T> result(num);
    T step = (stop - start) / static_cast<T>(num - 1);
    
    for (size_t i = 0; i < num; ++i) {
        result[i] = start + static_cast<T>(i) * step;
    }
    
    return result;
}

template<typename T>
std::vector<T> arange(T start, T stop, T step) {
    if (step == 0) throw std::invalid_argument("Step cannot be zero");
    
    if ((step > 0 && start >= stop) || (step < 0 && start <= stop)) {
        return {};
    }
    
    size_t num = static_cast<size_t>((stop - start) / step);
    std::vector<T> result(num);
    
    for (size_t i = 0; i < num; ++i) {
        result[i] = start + static_cast<T>(i) * step;
    }
    
    return result;
}

} // namespace stats
} // namespace df

#endif // DF_DS_LIBRARY_STATS_H