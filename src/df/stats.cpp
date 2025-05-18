#include "df/stats.hpp"
#include "df/dataframe.hpp"
#include <algorithm>
#include <cmath>
#include <numeric>
#include <limits>

namespace df { namespace stats {

Value mean(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;

        size_t count = 0;
        double sum = 0.0;

        for (const auto& val : vec) {
            if constexpr (std::is_same_v<T, NullableInt> ||
                          std::is_same_v<T, NullableDouble> ||
                          std::is_same_v<T, NullableBool>) {
                if (!val.isNA()) {
                    if constexpr (std::is_same_v<T, NullableBool>) {
                        sum += val.valueUnsafe() ? 1.0 : 0.0;
                    } else {
                        sum += static_cast<double>(val.valueUnsafe());
                    }
                    count++;
                }
            }
        }

        if (count == 0) return NA_VALUE;
        return sum / count;
    }, column);
}

Value sum(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;

        if constexpr (std::is_same_v<T, NullableInt>) {
            long long total = 0;
            bool anyValid = false;
            for (const auto& val : vec) {
                if (!val.isNA()) {
                    total += val.valueUnsafe();
                    anyValid = true;
                }
            }
            if (!anyValid) return NA_VALUE;
            if (total >= std::numeric_limits<int>::min() && total <= std::numeric_limits<int>::max()) {
                return static_cast<int>(total);
            }
            return static_cast<double>(total);
        }
        else if constexpr (std::is_same_v<T, NullableDouble>) {
            double total = 0.0;
            bool anyValid = false;
            for (const auto& val : vec) {
                if (!val.isNA()) {
                    total += val.valueUnsafe();
                    anyValid = true;
                }
            }
            if (!anyValid) return NA_VALUE;
            return total;
        }
        else if constexpr (std::is_same_v<T, NullableBool>) {
            int total = 0;
            bool anyValid = false;
            for (const auto& val : vec) {
                if (!val.isNA()) {
                    total += val.valueUnsafe() ? 1 : 0;
                    anyValid = true;
                }
            }
            if (!anyValid) return NA_VALUE;
            return total;
        }
        return NA_VALUE;
    }, column);
}

Value max(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;

        if (vec.empty()) return NA_VALUE;

        if constexpr (std::is_same_v<T, NullableInt> ||
                      std::is_same_v<T, NullableDouble> ||
                      std::is_same_v<T, NullableBool>) {
            auto it = std::max_element(vec.begin(), vec.end(),
                [](const auto& a, const auto& b) {
                    if (a.isNA()) return !b.isNA();
                    if (b.isNA()) return false;
                    return a.valueUnsafe() < b.valueUnsafe();
                });
            if (it == vec.end() || it->isNA()) return NA_VALUE;
            return it->valueUnsafe();
        }
        else if constexpr (std::is_same_v<T, NullableString>) {
            NullableString maxVal;
            bool anyValid = false;
            for (const auto& val : vec) {
                if (!val.isNA()) {
                    if (!anyValid || val.valueUnsafe() > maxVal.valueUnsafe()) {
                        maxVal = val;
                        anyValid = true;
                    }
                }
            }
            if (!anyValid) return NA_VALUE;
            return maxVal.valueUnsafe();
        }
        return NA_VALUE;
    }, column);
}

Value min(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;

        if (vec.empty()) return NA_VALUE;

        if constexpr (std::is_same_v<T, NullableInt> ||
                      std::is_same_v<T, NullableDouble> ||
                      std::is_same_v<T, NullableBool>) {
            auto it = std::min_element(vec.begin(), vec.end(),
                [](const auto& a, const auto& b) {
                    if (a.isNA()) return false;
                    if (b.isNA()) return true;
                    return a.valueUnsafe() < b.valueUnsafe();
                });
            if (it == vec.end() || it->isNA()) return NA_VALUE;
            return it->valueUnsafe();
        }
        else if constexpr (std::is_same_v<T, NullableString>) {
            NullableString minVal;
            bool anyValid = false;
            for (const auto& val : vec) {
                if (!val.isNA()) {
                    if (!anyValid || val.valueUnsafe() < minVal.valueUnsafe()) {
                        minVal = val;
                        anyValid = true;
                    }
                }
            }
            if (!anyValid) return NA_VALUE;
            return minVal.valueUnsafe();
        }
        return NA_VALUE;
    }, column);
}

Value median(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;

        if constexpr (std::is_same_v<T, NullableInt> || std::is_same_v<T, NullableDouble>) {
            std::vector<double> values;
            for (const auto& val : vec) {
                if (!val.isNA()) values.push_back(static_cast<double>(val.valueUnsafe()));
            }
            if (values.empty()) return NA_VALUE;
            std::sort(values.begin(), values.end());
            size_t n = values.size();
            if (n % 2 == 0) return (values[n / 2 - 1] + values[n / 2]) / 2.0;
            return values[n / 2];
        }
        return NA_VALUE;
    }, column);
}

Value count(const ColumnData& column) {
    return std::visit([](const auto& vec) -> Value {
        size_t validCount = 0;
        for (const auto& val : vec) {
            if (!val.isNA()) validCount++;
        }
        return static_cast<int>(validCount);
    }, column);
}

Value var(const ColumnData& column, size_t ddof) {
    return std::visit([ddof](const auto& vec) -> Value {
        using T = typename std::decay_t<decltype(vec)>::value_type;

        if constexpr (std::is_same_v<T, NullableInt> || std::is_same_v<T, NullableDouble>) {
            double meanVal = 0.0;
            size_t count = 0;
            for (const auto& val : vec) {
                if (!val.isNA()) {
                    meanVal += static_cast<double>(val.valueUnsafe());
                    count++;
                }
            }
            if (count <= ddof) return NA_VALUE;
            meanVal /= count;

            double sumSquaredDiff = 0.0;
            for (const auto& val : vec) {
                if (!val.isNA()) {
                    double diff = static_cast<double>(val.valueUnsafe()) - meanVal;
                    sumSquaredDiff += diff * diff;
                }
            }
            return sumSquaredDiff / (count - ddof);
        }
        return NA_VALUE;
    }, column);
}

Value std(const ColumnData& column, size_t ddof) {
    Value variance = var(column, ddof);
    if (std::holds_alternative<double>(variance)) {
        return std::sqrt(std::get<double>(variance));
    }
    return NA_VALUE;
}

namespace {

std::pair<std::vector<double>, std::vector<double>>
extractPairedNumeric(const ColumnData& col1, const ColumnData& col2) {
    auto toDouble = [](const ColumnData& c, size_t idx) -> std::pair<bool, double> {
        return std::visit([idx](const auto& vec) -> std::pair<bool, double> {
            using VecType = std::decay_t<decltype(vec)>;
            if constexpr (std::is_same_v<VecType, IntColumn> || std::is_same_v<VecType, DoubleColumn>) {
                if (vec[idx].isNA()) return {true, 0.0};
                return {false, static_cast<double>(vec[idx].valueUnsafe())};
            } else {
                return {true, 0.0};
            }
        }, c);
    };

    size_t colSize = std::visit([](const auto& v) { return v.size(); }, col1);
    std::vector<double> vals1, vals2;
    for (size_t i = 0; i < colSize; ++i) {
        auto [na1, v1] = toDouble(col1, i);
        auto [na2, v2] = toDouble(col2, i);
        if (!na1 && !na2) {
            vals1.push_back(v1);
            vals2.push_back(v2);
        }
    }
    return {vals1, vals2};
}

std::vector<std::string> numericColumnNames(const DataFrame& df) {
    std::vector<std::string> names;
    for (const auto& name : df.getColumnNames()) {
        const auto& colData = df[name];
        if (std::holds_alternative<IntColumn>(colData) ||
            std::holds_alternative<DoubleColumn>(colData)) {
            names.push_back(name);
        }
    }
    return names;
}

} // namespace

DataFrame corr(const DataFrame& df) {
    std::vector<std::pair<std::string, ColumnData>> result;
    auto names = numericColumnNames(df);

    for (const auto& a : names) {
        DoubleColumn col;
        col.reserve(names.size());
        for (const auto& b : names) {
            if (a == b) { col.push_back(1.0); continue; }

            auto [vals1, vals2] = extractPairedNumeric(df[a], df[b]);

            if (vals1.size() <= 1) {
                col.push_back(NullableDouble(std::numeric_limits<double>::quiet_NaN()));
                continue;
            }

            double m1 = std::accumulate(vals1.begin(), vals1.end(), 0.0) / vals1.size();
            double m2 = std::accumulate(vals2.begin(), vals2.end(), 0.0) / vals2.size();
            double num = 0.0, d1 = 0.0, d2 = 0.0;
            for (size_t i = 0; i < vals1.size(); ++i) {
                double diff1 = vals1[i] - m1;
                double diff2 = vals2[i] - m2;
                num += diff1 * diff2;
                d1  += diff1 * diff1;
                d2  += diff2 * diff2;
            }
            double r = (d1 * d2 > 0)
                ? num / std::sqrt(d1 * d2)
                : std::numeric_limits<double>::quiet_NaN();
            col.push_back(r);
        }
        result.emplace_back(a, col);
    }

    DataFrame out(result);
    if (!names.empty()) out.setIndex(names);
    return out;
}

DataFrame cov(const DataFrame& df) {
    std::vector<std::pair<std::string, ColumnData>> result;
    auto names = numericColumnNames(df);

    for (const auto& a : names) {
        DoubleColumn col;
        col.reserve(names.size());
        for (const auto& b : names) {
            if (a == b) {
                Value v = var(df[a]);
                if (std::holds_alternative<double>(v)) {
                    col.push_back(std::get<double>(v));
                } else {
                    col.push_back(NullableDouble(std::numeric_limits<double>::quiet_NaN()));
                }
                continue;
            }

            auto [vals1, vals2] = extractPairedNumeric(df[a], df[b]);

            if (vals1.size() <= 1) {
                col.push_back(NullableDouble(std::numeric_limits<double>::quiet_NaN()));
                continue;
            }

            double m1 = std::accumulate(vals1.begin(), vals1.end(), 0.0) / vals1.size();
            double m2 = std::accumulate(vals2.begin(), vals2.end(), 0.0) / vals2.size();
            double cov = 0.0;
            for (size_t i = 0; i < vals1.size(); ++i) {
                cov += (vals1[i] - m1) * (vals2[i] - m2);
            }
            cov /= (vals1.size() - 1);
            col.push_back(cov);
        }
        result.emplace_back(a, col);
    }

    DataFrame out(result);
    if (!names.empty()) out.setIndex(names);
    return out;
}

}} // namespace df::stats
