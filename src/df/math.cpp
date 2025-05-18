#include "df/math.hpp"
#include "df/dataframe.hpp"
#include <stdexcept>
#include <type_traits>
#include <optional>
#include <algorithm>

namespace {

df::IntColumn boolToInt(const df::BoolColumn& boolVec) {
    df::IntColumn result;
    result.reserve(boolVec.size());
    for (const auto& elem : boolVec) {
        if (elem.isNA()) result.push_back(df::NA_VALUE);
        else result.push_back(elem.valueUnsafe() ? 1 : 0);
    }
    return result;
}

df::DoubleColumn intToDouble(const df::IntColumn& intVec) {
    df::DoubleColumn result;
    result.reserve(intVec.size());
    for (const auto& elem : intVec) {
        if (elem.isNA()) result.push_back(df::NA_VALUE);
        else result.push_back(static_cast<double>(elem.valueUnsafe()));
    }
    return result;
}

bool isIntLikeColumn(const df::ColumnData& col) {
    return std::holds_alternative<df::IntColumn>(col) ||
           std::holds_alternative<df::BoolColumn>(col);
}

bool isNumericLikeColumn(const df::ColumnData& col) {
    return isIntLikeColumn(col) || std::holds_alternative<df::DoubleColumn>(col);
}

df::IntColumn toIntColumn(const df::ColumnData& col) {
    if (std::holds_alternative<df::IntColumn>(col)) return std::get<df::IntColumn>(col);
    if (std::holds_alternative<df::BoolColumn>(col)) return boolToInt(std::get<df::BoolColumn>(col));
    throw std::invalid_argument("Column is not int-like.");
}

df::DoubleColumn toDoubleColumn(const df::ColumnData& col) {
    if (std::holds_alternative<df::DoubleColumn>(col)) return std::get<df::DoubleColumn>(col);
    return intToDouble(toIntColumn(col));
}

std::optional<int> getIntFillValue(const df::Value& fill) {
    if (std::holds_alternative<int>(fill)) return std::get<int>(fill);
    if (std::holds_alternative<double>(fill)) return static_cast<int>(std::get<double>(fill));
    return std::nullopt;
}

std::optional<double> getDoubleFillValue(const df::Value& fill) {
    if (std::holds_alternative<double>(fill)) return std::get<double>(fill);
    if (std::holds_alternative<int>(fill)) return static_cast<double>(std::get<int>(fill));
    return std::nullopt;
}

template<typename Vec, typename Fill, typename Op>
void applyVecOp(Vec& result, const Vec& other, std::optional<Fill> fillOpt, Op op) {
    size_t minSize = std::min(result.size(), other.size());
    for (size_t i = 0; i < minSize; ++i) {
        if (!result[i].isNA() && !other[i].isNA()) {
            op(result[i], result[i].valueUnsafe(), other[i].valueUnsafe());
        } else if (fillOpt.has_value()) {
            Fill f = fillOpt.value();
            if (result[i].isNA() && other[i].isNA()) {
                // both NA, stays NA
            } else if (result[i].isNA()) {
                op(result[i], f, other[i].valueUnsafe());
            } else {
                op(result[i], result[i].valueUnsafe(), f);
            }
        } else {
            result[i] = df::NA_VALUE;
        }
    }
    for (size_t i = minSize; i < result.size(); ++i) {
        if (fillOpt.has_value() && !result[i].isNA()) {
            op(result[i], result[i].valueUnsafe(), fillOpt.value());
        } else {
            result[i] = df::NA_VALUE;
        }
    }
}

template<typename Op>
df::DataFrame applyDfDfOp(const df::DataFrame& df, const df::DataFrame& other,
                          const df::Value& fillValue, Op op) {
    df::DataFrame result = df;
    for (const auto& [colName, otherData] : other.getColumns()) {
        if (!df.columnExists(colName)) {
            result.addColumn(colName, otherData);
            continue;
        }
        const auto& selfData = df[colName];

        if (isIntLikeColumn(selfData) && isIntLikeColumn(otherData)) {
            df::IntColumn selfVec = toIntColumn(selfData);
            df::IntColumn otherVec = toIntColumn(otherData);
            applyVecOp(selfVec, otherVec, getIntFillValue(fillValue), op);
            result[colName] = df::ColumnData(selfVec);
        } else if (isNumericLikeColumn(selfData) && isNumericLikeColumn(otherData)) {
            df::DoubleColumn selfVec = toDoubleColumn(selfData);
            df::DoubleColumn otherVec = toDoubleColumn(otherData);
            applyVecOp(selfVec, otherVec, getDoubleFillValue(fillValue), op);
            result[colName] = df::ColumnData(selfVec);
        }
    }
    return result;
}

template<typename Op>
void scalarInPlace(df::DataFrame& df, const std::string& columnName,
                   const df::Value& value, Op op) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }
    auto& colData = df[columnName];

    if (std::holds_alternative<double>(value)) {
        double val = std::get<double>(value);
        if (std::holds_alternative<df::IntColumn>(colData)) {
            auto& intVec = std::get<df::IntColumn>(colData);
            df::DoubleColumn doubleVec;
            doubleVec.reserve(intVec.size());
            for (const auto& elem : intVec) {
                if (elem.isNA()) {
                    doubleVec.push_back(df::NA_VALUE);
                } else {
                    df::NullableDouble d;
                    op(d, static_cast<double>(elem.valueUnsafe()), val);
                    doubleVec.push_back(d);
                }
            }
            colData = doubleVec;
        } else {
            std::visit([&](auto& vec) {
                using V = typename std::decay_t<decltype(vec)>::value_type;
                if constexpr (std::is_same_v<V, df::NullableDouble>) {
                    for (auto& elem : vec) {
                        if (elem.isNA()) continue;
                        op(elem, elem.valueUnsafe(), val);
                    }
                } else if constexpr (std::is_same_v<V, df::NullableBool> ||
                                     std::is_same_v<V, df::NullableString>) {
                    throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
                }
            }, colData);
        }
    } else if (std::holds_alternative<int>(value)) {
        int val = std::get<int>(value);
        std::visit([&](auto& vec) {
            using V = typename std::decay_t<decltype(vec)>::value_type;
            if constexpr (std::is_same_v<V, df::NullableInt>) {
                for (auto& elem : vec) {
                    if (elem.isNA()) continue;
                    op(elem, elem.valueUnsafe(), val);
                }
            } else if constexpr (std::is_same_v<V, df::NullableDouble>) {
                for (auto& elem : vec) {
                    if (elem.isNA()) continue;
                    op(elem, elem.valueUnsafe(), static_cast<double>(val));
                }
            } else if constexpr (std::is_same_v<V, df::NullableBool> ||
                                 std::is_same_v<V, df::NullableString>) {
                throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
            }
        }, colData);
    } else {
        throw std::invalid_argument("Unsupported value type for arithmetic operation.");
    }
}

template<typename InPlaceOp>
df::DataFrame applyScalar(const df::DataFrame& df, const df::Value& value, InPlaceOp inPlaceOp) {
    df::DataFrame result = df;
    for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<df::IntColumn>(colData) ||
            std::holds_alternative<df::DoubleColumn>(colData)) {
            inPlaceOp(result, colName, value);
        } else if (std::holds_alternative<df::BoolColumn>(colData)) {
            result[colName] = boolToInt(std::get<df::BoolColumn>(colData));
            inPlaceOp(result, colName, value);
        }
    }
    return result;
}

constexpr auto opAdd = [](auto& dst, auto a, auto b) { dst = a + b; };
constexpr auto opSub = [](auto& dst, auto a, auto b) { dst = a - b; };
constexpr auto opMul = [](auto& dst, auto a, auto b) { dst = a * b; };
constexpr auto opDiv = [](auto& dst, auto a, auto b) {
    if (b == 0) dst = df::NA_VALUE;
    else dst = a / b;
};

bool valueIsZero(const df::Value& v) {
    if (std::holds_alternative<double>(v)) return std::get<double>(v) == 0;
    if (std::holds_alternative<int>(v)) return std::get<int>(v) == 0;
    return false;
}

} // anonymous namespace

namespace df { namespace math {

void addInPlace(DataFrame& df, const std::string& columnName, const Value& value) {
    scalarInPlace(df, columnName, value, opAdd);
}

void subtractInPlace(DataFrame& df, const std::string& columnName, const Value& value) {
    scalarInPlace(df, columnName, value, opSub);
}

void multiplyInPlace(DataFrame& df, const std::string& columnName, const Value& value) {
    scalarInPlace(df, columnName, value, opMul);
}

void divideInPlace(DataFrame& df, const std::string& columnName, const Value& value) {
    if (valueIsZero(value)) throw std::invalid_argument("Division by zero.");
    scalarInPlace(df, columnName, value, opDiv);
}

DataFrame add(const DataFrame& df, const DataFrame& other, const Value& fillValue) {
    return applyDfDfOp(df, other, fillValue, opAdd);
}

DataFrame subtract(const DataFrame& df, const DataFrame& other, const Value& fillValue) {
    return applyDfDfOp(df, other, fillValue, opSub);
}

DataFrame multiply(const DataFrame& df, const DataFrame& other, const Value& fillValue) {
    return applyDfDfOp(df, other, fillValue, opMul);
}

DataFrame divide(const DataFrame& df, const DataFrame& other, const Value& fillValue) {
    return applyDfDfOp(df, other, fillValue, opDiv);
}

DataFrame add(const DataFrame& df, const Value& value) {
    return applyScalar(df, value, addInPlace);
}

DataFrame subtract(const DataFrame& df, const Value& value) {
    return applyScalar(df, value, subtractInPlace);
}

DataFrame multiply(const DataFrame& df, const Value& value) {
    return applyScalar(df, value, multiplyInPlace);
}

DataFrame divide(const DataFrame& df, const Value& value) {
    if (valueIsZero(value)) throw std::invalid_argument("Division by zero.");
    return applyScalar(df, value, divideInPlace);
}

}} // namespace df::math
