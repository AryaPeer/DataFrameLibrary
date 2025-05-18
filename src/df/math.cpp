#include "df/math.hpp"
#include "df/dataframe.hpp"
#include <stdexcept>
#include <type_traits>
#include <optional>

namespace {

// Convert BoolColumn to IntColumn, preserving NAs
df::IntColumn boolToInt(const df::BoolColumn& boolVec) {
    df::IntColumn result;
    result.reserve(boolVec.size());
    for (const auto& elem : boolVec) {
        if (elem.is_na()) {
            result.push_back(df::NA_VALUE);
        } else {
            result.push_back(elem.value_unsafe() ? 1 : 0);
        }
    }
    return result;
}

// Convert IntColumn to DoubleColumn, preserving NAs (used for mixed-type promotion)
df::DoubleColumn intToDouble(const df::IntColumn& intVec) {
    df::DoubleColumn result;
    result.reserve(intVec.size());
    for (const auto& elem : intVec) {
        if (elem.is_na()) {
            result.push_back(df::NA_VALUE);
        } else {
            result.push_back(static_cast<double>(elem.value_unsafe()));
        }
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
    if (std::holds_alternative<df::IntColumn>(col)) {
        return std::get<df::IntColumn>(col);
    }
    if (std::holds_alternative<df::BoolColumn>(col)) {
        return boolToInt(std::get<df::BoolColumn>(col));
    }
    throw std::invalid_argument("Column is not int-like.");
}

df::DoubleColumn toDoubleColumn(const df::ColumnData& col) {
    if (std::holds_alternative<df::DoubleColumn>(col)) {
        return std::get<df::DoubleColumn>(col);
    }
    return intToDouble(toIntColumn(col));
}

// Extract fill value as int, accepting both int and double
std::optional<int> getIntFillValue(const df::Value& fill_value) {
    if (std::holds_alternative<int>(fill_value)) return std::get<int>(fill_value);
    if (std::holds_alternative<double>(fill_value)) return static_cast<int>(std::get<double>(fill_value));
    return std::nullopt;
}

// Extract fill value as double, accepting both int and double
std::optional<double> getDoubleFillValue(const df::Value& fill_value) {
    if (std::holds_alternative<double>(fill_value)) return std::get<double>(fill_value);
    if (std::holds_alternative<int>(fill_value)) return static_cast<double>(std::get<int>(fill_value));
    return std::nullopt;
}

} // anonymous namespace

namespace df {
namespace math {

void add_inplace(DataFrame& df, const std::string& columnName, const Value& value) {
    if (!df.columnExists(columnName)) {
        throw std::out_of_range("Column does not exist.");
    }

    auto& colData = df[columnName];

    if (std::holds_alternative<double>(value)) {
        double val = std::get<double>(value);

        if (std::holds_alternative<IntColumn>(colData)) {
            auto& intVec = std::get<IntColumn>(colData);
            DoubleColumn doubleVec;
            doubleVec.reserve(intVec.size());
            for (const auto& elem : intVec) {
                if (elem.is_na()) {
                    doubleVec.push_back(NA_VALUE);
                } else {
                    doubleVec.push_back(static_cast<double>(elem.value_unsafe()) + val);
                }
            }
            colData = doubleVec;
        } else {
            std::visit(
                [val](auto&& vec) {
                    using VecType = std::decay_t<decltype(vec)>;
                    using ValueType = typename VecType::value_type;

                    if constexpr (std::is_same_v<ValueType, NullableDouble>) {
                        for (auto& elem : vec) {
                            if (!elem.is_na()) {
                                elem += val;
                            }
                        }
                    } else if constexpr (std::is_same_v<ValueType, NullableBool> ||
                                         std::is_same_v<ValueType, NullableString>) {
                        throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
                    }
                },
                colData);
        }

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
                } else if constexpr (std::is_same_v<ValueType, NullableBool> ||
                                     std::is_same_v<ValueType, NullableString>) {
                    throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
                }
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

    if (std::holds_alternative<double>(value)) {
        double val = std::get<double>(value);

        if (std::holds_alternative<IntColumn>(colData)) {
            auto& intVec = std::get<IntColumn>(colData);
            DoubleColumn doubleVec;
            doubleVec.reserve(intVec.size());
            for (const auto& elem : intVec) {
                if (elem.is_na()) {
                    doubleVec.push_back(NA_VALUE);
                } else {
                    doubleVec.push_back(static_cast<double>(elem.value_unsafe()) - val);
                }
            }
            colData = doubleVec;
        } else {
            std::visit(
                [val](auto&& vec) {
                    using VecType = std::decay_t<decltype(vec)>;
                    using ValueType = typename VecType::value_type;

                    if constexpr (std::is_same_v<ValueType, NullableDouble>) {
                        for (auto& elem : vec) {
                            if (!elem.is_na()) {
                                elem -= val;
                            }
                        }
                    } else if constexpr (std::is_same_v<ValueType, NullableBool> ||
                                         std::is_same_v<ValueType, NullableString>) {
                        throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
                    }
                },
                colData);
        }

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
                } else if constexpr (std::is_same_v<ValueType, NullableBool> ||
                                     std::is_same_v<ValueType, NullableString>) {
                    throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
                }
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
    if (std::holds_alternative<double>(value)) {
        double val = std::get<double>(value);
        if (std::holds_alternative<IntColumn>(colData)) {
            auto& intVec = std::get<IntColumn>(colData);
            DoubleColumn doubleVec;
            doubleVec.reserve(intVec.size());
            for (const auto& elem : intVec) {
                if (elem.is_na()) {
                    doubleVec.push_back(NA_VALUE);
                } else {
                    doubleVec.push_back(static_cast<double>(elem.value_unsafe()) * val);
                }
            }
            colData = doubleVec;
        } else {
            std::visit(
                [val](auto&& vec) {
                    using VecType = std::decay_t<decltype(vec)>;
                    using ValueType = typename VecType::value_type;
                    if constexpr (std::is_same_v<ValueType, NullableDouble>) {
                        for (auto& elem : vec) {
                            if (!elem.is_na()) {
                                elem *= val;
                            }
                        }
                    } else if constexpr (std::is_same_v<ValueType, NullableBool> ||
                                         std::is_same_v<ValueType, NullableString>) {
                        throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
                    }
                },
                colData);
        }
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
                } else if constexpr (std::is_same_v<ValueType, NullableBool> ||
                                     std::is_same_v<ValueType, NullableString>) {
                    throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
                }
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

    if ((std::holds_alternative<double>(value) && std::get<double>(value) == 0) ||
        (std::holds_alternative<int>(value) && std::get<int>(value) == 0)) {
        throw std::invalid_argument("Division by zero.");
    }

    auto& colData = df[columnName];
    if (std::holds_alternative<double>(value)) {
        double val = std::get<double>(value);
        if (std::holds_alternative<IntColumn>(colData)) {
            auto& intVec = std::get<IntColumn>(colData);
            DoubleColumn doubleVec;
            doubleVec.reserve(intVec.size());
            for (const auto& elem : intVec) {
                if (elem.is_na()) {
                    doubleVec.push_back(NA_VALUE);
                } else {
                    doubleVec.push_back(static_cast<double>(elem.value_unsafe()) / val);
                }
            }
            colData = doubleVec;
        } else {
            std::visit(
                [val](auto&& vec) {
                    using VecType = std::decay_t<decltype(vec)>;
                    using ValueType = typename VecType::value_type;
                    if constexpr (std::is_same_v<ValueType, NullableDouble>) {
                        for (auto& elem : vec) {
                            if (!elem.is_na()) {
                                elem /= val;
                            }
                        }
                    } else if constexpr (std::is_same_v<ValueType, NullableBool> ||
                                         std::is_same_v<ValueType, NullableString>) {
                        throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
                    }
                },
                colData);
        }
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
                } else if constexpr (std::is_same_v<ValueType, NullableBool> ||
                                     std::is_same_v<ValueType, NullableString>) {
                    throw std::invalid_argument("Arithmetic operation not supported for non-numeric column type.");
                }
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
            auto resultCol = df[colName];
            auto otherCol = other[colName];

            bool resultIsIntLike = isIntLikeColumn(resultCol);
            bool otherIsIntLike = isIntLikeColumn(otherCol);

            if (resultIsIntLike && otherIsIntLike) {
                // Int/bool combination remains IntColumn
                IntColumn resultVec = toIntColumn(resultCol);
                const IntColumn otherVec = toIntColumn(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                auto fillOpt = getIntFillValue(fill_value);

                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() + otherVec[i].value_unsafe();
                    } else if (fillOpt.has_value()) {
                        int fillVal = fillOpt.value();
                        if (resultVec[i].is_na() && otherVec[i].is_na()) {
                            // Both NA: stays NA
                        } else if (resultVec[i].is_na()) {
                            resultVec[i] = fillVal + otherVec[i].value_unsafe();
                        } else {
                            resultVec[i] = resultVec[i].value_unsafe() + fillVal;
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                // Trailing rows beyond other's length — treat as NA
                for (size_t i = minSize; i < resultVec.size(); ++i) {
                    if (fillOpt.has_value() && !resultVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() + fillOpt.value();
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                result[colName] = ColumnData(resultVec);
            } else if (isNumericLikeColumn(resultCol) && isNumericLikeColumn(otherCol)) {
                // Any mixed numeric/bool combination with a double promotes to DoubleColumn
                DoubleColumn resultVec = toDoubleColumn(resultCol);
                const DoubleColumn otherVec = toDoubleColumn(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                auto fillOpt = getDoubleFillValue(fill_value);

                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() + otherVec[i].value_unsafe();
                    } else if (fillOpt.has_value()) {
                        double fillVal = fillOpt.value();
                        if (resultVec[i].is_na() && otherVec[i].is_na()) {
                            // Both NA: stays NA
                        } else if (resultVec[i].is_na()) {
                            resultVec[i] = fillVal + otherVec[i].value_unsafe();
                        } else {
                            resultVec[i] = resultVec[i].value_unsafe() + fillVal;
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                // Trailing rows beyond other's length — treat as NA
                for (size_t i = minSize; i < resultVec.size(); ++i) {
                    if (fillOpt.has_value() && !resultVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() + fillOpt.value();
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                result[colName] = ColumnData(resultVec);
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
            auto resultCol = df[colName];
            auto otherCol = other[colName];

            bool resultIsIntLike = isIntLikeColumn(resultCol);
            bool otherIsIntLike = isIntLikeColumn(otherCol);

            if (resultIsIntLike && otherIsIntLike) {
                // Int/bool combination remains IntColumn
                IntColumn resultVec = toIntColumn(resultCol);
                const IntColumn otherVec = toIntColumn(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                auto fillOpt = getIntFillValue(fill_value);

                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() - otherVec[i].value_unsafe();
                    } else if (fillOpt.has_value()) {
                        int fillVal = fillOpt.value();
                        if (resultVec[i].is_na() && otherVec[i].is_na()) {
                            // Both NA: stays NA
                        } else if (resultVec[i].is_na()) {
                            resultVec[i] = fillVal - otherVec[i].value_unsafe();
                        } else {
                            resultVec[i] = resultVec[i].value_unsafe() - fillVal;
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                // Trailing rows beyond other's length — treat as NA
                for (size_t i = minSize; i < resultVec.size(); ++i) {
                    if (fillOpt.has_value() && !resultVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() - fillOpt.value();
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                result[colName] = ColumnData(resultVec);
            } else if (isNumericLikeColumn(resultCol) && isNumericLikeColumn(otherCol)) {
                // Any mixed numeric/bool combination with a double promotes to DoubleColumn
                DoubleColumn resultVec = toDoubleColumn(resultCol);
                const DoubleColumn otherVec = toDoubleColumn(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                auto fillOpt = getDoubleFillValue(fill_value);

                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() - otherVec[i].value_unsafe();
                    } else if (fillOpt.has_value()) {
                        double fillVal = fillOpt.value();
                        if (resultVec[i].is_na() && otherVec[i].is_na()) {
                            // Both NA: stays NA
                        } else if (resultVec[i].is_na()) {
                            resultVec[i] = fillVal - otherVec[i].value_unsafe();
                        } else {
                            resultVec[i] = resultVec[i].value_unsafe() - fillVal;
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                // Trailing rows beyond other's length — treat as NA
                for (size_t i = minSize; i < resultVec.size(); ++i) {
                    if (fillOpt.has_value() && !resultVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() - fillOpt.value();
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                result[colName] = ColumnData(resultVec);
            }
        } else {
            // Preserve columns unique to other
            result.addColumn(colName, colData);
        }
    }

    return result;
}

DataFrame multiply(const DataFrame& df, const DataFrame& other, const Value& fill_value) {
    DataFrame result = df;

    for (const auto& [colName, colData] : other.getColumns()) {
        if (df.columnExists(colName)) {
            auto resultCol = df[colName];
            auto otherCol = other[colName];

            bool resultIsIntLike = isIntLikeColumn(resultCol);
            bool otherIsIntLike = isIntLikeColumn(otherCol);

            if (resultIsIntLike && otherIsIntLike) {
                // Int/bool combination remains IntColumn
                IntColumn resultVec = toIntColumn(resultCol);
                const IntColumn otherVec = toIntColumn(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                auto fillOpt = getIntFillValue(fill_value);

                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() * otherVec[i].value_unsafe();
                    } else if (fillOpt.has_value()) {
                        int fillVal = fillOpt.value();
                        if (resultVec[i].is_na() && otherVec[i].is_na()) {
                            // Both NA: stays NA
                        } else if (resultVec[i].is_na()) {
                            resultVec[i] = fillVal * otherVec[i].value_unsafe();
                        } else {
                            resultVec[i] = resultVec[i].value_unsafe() * fillVal;
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                // Trailing rows beyond other's length — treat as NA
                for (size_t i = minSize; i < resultVec.size(); ++i) {
                    if (fillOpt.has_value() && !resultVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() * fillOpt.value();
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                result[colName] = ColumnData(resultVec);
            } else if (isNumericLikeColumn(resultCol) && isNumericLikeColumn(otherCol)) {
                // Any mixed numeric/bool combination with a double promotes to DoubleColumn
                DoubleColumn resultVec = toDoubleColumn(resultCol);
                const DoubleColumn otherVec = toDoubleColumn(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                auto fillOpt = getDoubleFillValue(fill_value);

                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() * otherVec[i].value_unsafe();
                    } else if (fillOpt.has_value()) {
                        double fillVal = fillOpt.value();
                        if (resultVec[i].is_na() && otherVec[i].is_na()) {
                            // Both NA: stays NA
                        } else if (resultVec[i].is_na()) {
                            resultVec[i] = fillVal * otherVec[i].value_unsafe();
                        } else {
                            resultVec[i] = resultVec[i].value_unsafe() * fillVal;
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                // Trailing rows beyond other's length — treat as NA
                for (size_t i = minSize; i < resultVec.size(); ++i) {
                    if (fillOpt.has_value() && !resultVec[i].is_na()) {
                        resultVec[i] = resultVec[i].value_unsafe() * fillOpt.value();
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                result[colName] = ColumnData(resultVec);
            }
        } else {
            // Preserve columns unique to other
            result.addColumn(colName, colData);
        }
    }

    return result;
}

DataFrame divide(const DataFrame& df, const DataFrame& other, const Value& fill_value) {
    DataFrame result = df;

    for (const auto& [colName, colData] : other.getColumns()) {
        if (df.columnExists(colName)) {
            auto resultCol = df[colName];
            auto otherCol = other[colName];

            bool resultIsIntLike = isIntLikeColumn(resultCol);
            bool otherIsIntLike = isIntLikeColumn(otherCol);

            if (resultIsIntLike && otherIsIntLike) {
                // Int/bool combination remains IntColumn
                IntColumn resultVec = toIntColumn(resultCol);
                const IntColumn otherVec = toIntColumn(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                auto fillOpt = getIntFillValue(fill_value);

                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na() && otherVec[i].value_unsafe() != 0) {
                        resultVec[i] = resultVec[i].value_unsafe() / otherVec[i].value_unsafe();
                    } else if (!resultVec[i].is_na() && !otherVec[i].is_na() && otherVec[i].value_unsafe() == 0) {
                        // Division by zero — always NA regardless of fill_value
                        resultVec[i] = NA_VALUE;
                    } else if (fillOpt.has_value()) {
                        int fillVal = fillOpt.value();
                        if (resultVec[i].is_na() && otherVec[i].is_na()) {
                            // Both NA: stays NA
                        } else if (resultVec[i].is_na()) {
                            if (!otherVec[i].is_na() && otherVec[i].value_unsafe() != 0) {
                                resultVec[i] = fillVal / otherVec[i].value_unsafe();
                            }
                        } else {
                            // result valid, other is NA — substitute fillVal for other
                            if (fillVal != 0) {
                                resultVec[i] = resultVec[i].value_unsafe() / fillVal;
                            } else {
                                resultVec[i] = NA_VALUE;
                            }
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                // Trailing rows beyond other's length — treat as NA
                for (size_t i = minSize; i < resultVec.size(); ++i) {
                    if (fillOpt.has_value() && !resultVec[i].is_na()) {
                        if (fillOpt.value() != 0) {
                            resultVec[i] = resultVec[i].value_unsafe() / fillOpt.value();
                        } else {
                            resultVec[i] = NA_VALUE;
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                result[colName] = ColumnData(resultVec);
            } else if (isNumericLikeColumn(resultCol) && isNumericLikeColumn(otherCol)) {
                // Any mixed numeric/bool combination with a double promotes to DoubleColumn
                DoubleColumn resultVec = toDoubleColumn(resultCol);
                const DoubleColumn otherVec = toDoubleColumn(otherCol);
                size_t minSize = std::min(resultVec.size(), otherVec.size());
                auto fillOpt = getDoubleFillValue(fill_value);

                for (size_t i = 0; i < minSize; ++i) {
                    if (!resultVec[i].is_na() && !otherVec[i].is_na() && otherVec[i].value_unsafe() != 0) {
                        resultVec[i] = resultVec[i].value_unsafe() / otherVec[i].value_unsafe();
                    } else if (!resultVec[i].is_na() && !otherVec[i].is_na() && otherVec[i].value_unsafe() == 0) {
                        // Division by zero — always NA regardless of fill_value
                        resultVec[i] = NA_VALUE;
                    } else if (fillOpt.has_value()) {
                        double fillVal = fillOpt.value();
                        if (resultVec[i].is_na() && otherVec[i].is_na()) {
                            // Both NA: stays NA
                        } else if (resultVec[i].is_na()) {
                            if (!otherVec[i].is_na() && otherVec[i].value_unsafe() != 0) {
                                resultVec[i] = fillVal / otherVec[i].value_unsafe();
                            }
                        } else {
                            // result valid, other is NA — substitute fillVal for other
                            if (fillVal != 0) {
                                resultVec[i] = resultVec[i].value_unsafe() / fillVal;
                            } else {
                                resultVec[i] = NA_VALUE;
                            }
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                // Trailing rows beyond other's length — treat as NA
                for (size_t i = minSize; i < resultVec.size(); ++i) {
                    if (fillOpt.has_value() && !resultVec[i].is_na()) {
                        if (fillOpt.value() != 0) {
                            resultVec[i] = resultVec[i].value_unsafe() / fillOpt.value();
                        } else {
                            resultVec[i] = NA_VALUE;
                        }
                    } else {
                        resultVec[i] = NA_VALUE;
                    }
                }

                result[colName] = ColumnData(resultVec);
            }
        } else {
            // Preserve columns unique to other
            result.addColumn(colName, colData);
        }
    }

    return result;
}

DataFrame add(const DataFrame& df, const Value& value) {
    DataFrame result = df;

    for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            add_inplace(result, colName, value);
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            result[colName] = boolToInt(std::get<BoolColumn>(colData));
            add_inplace(result, colName, value);
        }
    }

    return result;
}

DataFrame subtract(const DataFrame& df, const Value& value) {
    DataFrame result = df;

    for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            subtract_inplace(result, colName, value);
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            result[colName] = boolToInt(std::get<BoolColumn>(colData));
            subtract_inplace(result, colName, value);
        }
    }

    return result;
}

DataFrame multiply(const DataFrame& df, const Value& value) {
    DataFrame result = df;

    for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            multiply_inplace(result, colName, value);
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            result[colName] = boolToInt(std::get<BoolColumn>(colData));
            multiply_inplace(result, colName, value);
        }
    }

    return result;
}

DataFrame divide(const DataFrame& df, const Value& value) {
    if ((std::holds_alternative<double>(value) && std::get<double>(value) == 0) ||
        (std::holds_alternative<int>(value) && std::get<int>(value) == 0)) {
        throw std::invalid_argument("Division by zero.");
    }

    DataFrame result = df;

    for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            divide_inplace(result, colName, value);
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            result[colName] = boolToInt(std::get<BoolColumn>(colData));
            divide_inplace(result, colName, value);
        }
    }

    return result;
}

DataFrame operator+(const DataFrame& df, double value) {
    DataFrame result = df;
    for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            add_inplace(result, colName, value);
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            result[colName] = boolToInt(std::get<BoolColumn>(colData));
            add_inplace(result, colName, value);
        }
    }
    return result;
}

DataFrame operator-(const DataFrame& df, double value) {
    DataFrame result = df;
    for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            subtract_inplace(result, colName, value);
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            result[colName] = boolToInt(std::get<BoolColumn>(colData));
            subtract_inplace(result, colName, value);
        }
    }
    return result;
}

DataFrame operator*(const DataFrame& df, double value) {
    DataFrame result = df;
    for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            multiply_inplace(result, colName, value);
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            result[colName] = boolToInt(std::get<BoolColumn>(colData));
            multiply_inplace(result, colName, value);
        }
    }
    return result;
}

DataFrame operator/(const DataFrame& df, double value) {
    if (value == 0) {
        throw std::invalid_argument("Division by zero.");
    }

    DataFrame result = df;
    for (const auto& [colName, colData] : df.getColumns()) {
        if (std::holds_alternative<IntColumn>(colData) || std::holds_alternative<DoubleColumn>(colData)) {
            divide_inplace(result, colName, value);
        } else if (std::holds_alternative<BoolColumn>(colData)) {
            result[colName] = boolToInt(std::get<BoolColumn>(colData));
            divide_inplace(result, colName, value);
        }
    }
    return result;
}

} // namespace math
} // namespace df
