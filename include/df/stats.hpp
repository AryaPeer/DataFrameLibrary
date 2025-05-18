#ifndef DF_DS_LIBRARY_STATS_H
#define DF_DS_LIBRARY_STATS_H

#include "df/types.hpp"
#include <string>
#include <map>
#include <vector>
#include <cmath>
#include <limits>

namespace df {

// Forward declarations
class DataFrame;

namespace stats {

// Statistical summaries structure
struct DescribeResult {
    double count = 0.0;
    double mean = 0.0;
    double std = 0.0;
    double min = 0.0;
    double q25 = 0.0;
    double median = 0.0;
    double q75 = 0.0;
    double max = 0.0;
};

Value mean(const DataFrame& df, const std::string& columnName);
Value sum(const DataFrame& df, const std::string& columnName);
Value max(const DataFrame& df, const std::string& columnName);
Value min(const DataFrame& df, const std::string& columnName);
Value median(const DataFrame& df, const std::string& columnName);
Value var(const DataFrame& df, const std::string& columnName, size_t ddof = 1);
Value std(const DataFrame& df, const std::string& columnName, size_t ddof = 1);
Value count(const DataFrame& df, const std::string& columnName);

// Column-based statistics
Value mean(const ColumnData& column);
Value sum(const ColumnData& column);
Value max(const ColumnData& column);
Value min(const ColumnData& column);
Value median(const ColumnData& column);
Value var(const ColumnData& column, size_t ddof = 1);
Value std(const ColumnData& column, size_t ddof = 1);
Value count(const ColumnData& column);

// Statistical summaries
std::map<std::string, DescribeResult> describe(const DataFrame& df);
DescribeResult describe(const DataFrame& df, const std::string& columnName);

// Advanced statistics
DataFrame corr(const DataFrame& df);
DataFrame cov(const DataFrame& df);

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

    for (size_t i = 0; i < num; ++i) {
        result[i] = start + (stop - start) * static_cast<T>(i) / static_cast<T>(num - 1);
    }
    result[num - 1] = stop;

    return result;
}

template<typename T>
std::vector<T> arange(T start, T stop, T step) {
    if (step == 0) throw std::invalid_argument("Step cannot be zero");
    
    if ((step > 0 && start >= stop) || (step < 0 && start <= stop)) {
        return {};
    }
    
    double raw = static_cast<double>(stop - start) / static_cast<double>(step);
    double scale = std::max({std::abs(static_cast<double>(start)),
                             std::abs(static_cast<double>(stop)),
                             std::abs(static_cast<double>(step))});
    double eps = std::max(scale * std::numeric_limits<double>::epsilon() * 128,
                          std::abs(raw) * std::numeric_limits<double>::epsilon() * 128);
    size_t num = static_cast<size_t>(std::ceil(raw - eps));
    std::vector<T> result(num);
    
    for (size_t i = 0; i < num; ++i) {
        result[i] = start + static_cast<T>(i) * step;
    }
    
    return result;
}

} // namespace stats
} // namespace df

#endif // DF_DS_LIBRARY_STATS_H