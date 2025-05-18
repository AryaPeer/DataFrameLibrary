#ifndef DF_DS_LIBRARY_TYPES_H
#define DF_DS_LIBRARY_TYPES_H

#include <vector>
#include <string>
#include <variant>
#include <optional>
#include <cmath>
#include <limits>
#include <algorithm>
#include <map>
#include <stdexcept>
#include <type_traits>

namespace df {

struct NA {
    bool operator==(const NA&) const { return true; }
    bool operator!=(const NA&) const { return false; }
    bool operator<(const NA&) const { return false; }
};

inline constexpr NA NA_VALUE {};


template<typename T>
class Nullable {
private:
    std::optional<T> value;

public:
    Nullable() : value(std::nullopt) {}
    Nullable(const T& v) : value(v) {}
    Nullable(NA) : value(std::nullopt) {}

    // Converting constructor: allows e.g. const char* -> Nullable<std::string>
    template<typename U, typename = std::enable_if_t<
        std::is_constructible_v<T, const U&> &&
        !std::is_same_v<std::decay_t<U>, T> &&
        !std::is_same_v<std::decay_t<U>, Nullable> &&
        !std::is_same_v<std::decay_t<U>, NA>>>
    Nullable(const U& v) : value(T(v)) {}
    
    bool is_na() const { return !value.has_value(); }
    T value_or(T default_val) const { return value.value_or(default_val); }
    T value_unsafe() const { return value.value(); }
    
    // Operators with Nullable
    // NA == NA returns false, matching pandas NaN semantics (IEEE 754)
    bool operator==(const Nullable& other) const {
        if (is_na() || other.is_na()) return false;
        return value.value() == other.value.value();
    }
    
    bool operator!=(const Nullable& other) const {
        return !(*this == other);
    }
    
    // Operators with raw values
    bool operator==(const T& other) const {
        if (is_na()) return false;
        return value.value() == other;
    }
    
    bool operator!=(const T& other) const {
        return !(*this == other);
    }
    
    // Comparison operators
    bool operator<(const Nullable& other) const {
        if (is_na() || other.is_na()) return false;
        return value.value() < other.value.value();
    }
    
    bool operator>(const Nullable& other) const {
        if (is_na() || other.is_na()) return false;
        return value.value() > other.value.value();
    }
    
    bool operator<=(const Nullable& other) const {
        if (is_na() || other.is_na()) return false;
        return value.value() <= other.value.value();
    }
    
    bool operator>=(const Nullable& other) const {
        if (is_na() || other.is_na()) return false;
        return value.value() >= other.value.value();
    }
    
    // Comparison with raw values
    bool operator<(const T& other) const {
        if (is_na()) return false;
        return value.value() < other;
    }
    
    bool operator>(const T& other) const {
        if (is_na()) return false;
        return value.value() > other;
    }
    
    bool operator<=(const T& other) const {
        if (is_na()) return false;
        return value.value() <= other;
    }
    
    bool operator>=(const T& other) const {
        if (is_na()) return false;
        return value.value() >= other;
    }
    
    // Arithmetic operators (only enabled for arithmetic types via SFINAE)
    template<typename U = T>
    auto operator+(const Nullable& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (is_na() || other.is_na()) return NA_VALUE;
        return Nullable(value.value() + other.value.value());
    }

    template<typename U = T>
    auto operator-(const Nullable& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (is_na() || other.is_na()) return NA_VALUE;
        return Nullable(value.value() - other.value.value());
    }

    template<typename U = T>
    auto operator*(const Nullable& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (is_na() || other.is_na()) return NA_VALUE;
        return Nullable(value.value() * other.value.value());
    }

    template<typename U = T>
    auto operator/(const Nullable& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (is_na() || other.is_na() || other.value.value() == static_cast<T>(0)) return NA_VALUE;
        return Nullable(value.value() / other.value.value());
    }

    // Arithmetic operations with raw values
    template<typename U = T>
    auto operator+(const T& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (is_na()) return NA_VALUE;
        return Nullable(value.value() + other);
    }

    template<typename U = T>
    auto operator-(const T& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (is_na()) return NA_VALUE;
        return Nullable(value.value() - other);
    }

    template<typename U = T>
    auto operator*(const T& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (is_na()) return NA_VALUE;
        return Nullable(value.value() * other);
    }

    template<typename U = T>
    auto operator/(const T& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (is_na() || other == static_cast<T>(0)) return NA_VALUE;
        return Nullable(value.value() / other);
    }

    // Compound assignment operators
    template<typename U = T>
    auto operator+=(const Nullable& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (is_na() || other.is_na()) {
            value = std::nullopt;
        } else {
            value = value.value() + other.value.value();
        }
        return *this;
    }

    template<typename U = T>
    auto operator-=(const Nullable& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (is_na() || other.is_na()) {
            value = std::nullopt;
        } else {
            value = value.value() - other.value.value();
        }
        return *this;
    }

    template<typename U = T>
    auto operator*=(const Nullable& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (is_na() || other.is_na()) {
            value = std::nullopt;
        } else {
            value = value.value() * other.value.value();
        }
        return *this;
    }

    template<typename U = T>
    auto operator/=(const Nullable& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (is_na() || other.is_na() || other.value.value() == static_cast<T>(0)) {
            value = std::nullopt;
        } else {
            value = value.value() / other.value.value();
        }
        return *this;
    }

    // Compound assignment with raw values
    template<typename U = T>
    auto operator+=(const T& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (is_na()) return *this;
        value = value.value() + other;
        return *this;
    }

    template<typename U = T>
    auto operator-=(const T& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (is_na()) return *this;
        value = value.value() - other;
        return *this;
    }

    template<typename U = T>
    auto operator*=(const T& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (is_na()) return *this;
        value = value.value() * other;
        return *this;
    }

    template<typename U = T>
    auto operator/=(const T& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (is_na() || other == static_cast<T>(0)) {
            value = std::nullopt;
        } else {
            value = value.value() / other;
        }
        return *this;
    }
};

using NullableInt = Nullable<int>;
using NullableDouble = Nullable<double>;
using NullableBool = Nullable<bool>;
using NullableString = Nullable<std::string>;


using Value = std::variant<int, double, bool, std::string, NullableInt, NullableDouble, NullableBool, NullableString, NA>;

using IntColumn = std::vector<NullableInt>;
using DoubleColumn = std::vector<NullableDouble>;
using BoolColumn = std::vector<NullableBool>;
using StringColumn = std::vector<NullableString>;

using ColumnData = std::variant<IntColumn, DoubleColumn, BoolColumn, StringColumn>;

enum class DataType {
    Integer,
    Double,
    Boolean,
    String
};

inline std::string dataTypeToString(DataType type) {
    switch (type) {
        case DataType::Integer: return "int";
        case DataType::Double: return "double";
        case DataType::Boolean: return "bool";
        case DataType::String: return "string";
        default: return "unknown";
    }
}

inline DataType stringToDataType(const std::string& typeStr) {
    std::string lower = typeStr;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (lower == "int" || lower == "integer") return DataType::Integer;
    if (lower == "double" || lower == "float") return DataType::Double;
    if (lower == "bool" || lower == "boolean") return DataType::Boolean;
    if (lower == "string" || lower == "str") return DataType::String;
    throw std::invalid_argument("Unknown data type: " + typeStr);
}

inline DataType getDataType(const ColumnData& data) {
    if (std::holds_alternative<IntColumn>(data)) return DataType::Integer;
    if (std::holds_alternative<DoubleColumn>(data)) return DataType::Double;
    if (std::holds_alternative<BoolColumn>(data)) return DataType::Boolean;
    if (std::holds_alternative<StringColumn>(data)) return DataType::String;
    throw std::invalid_argument("Unable to determine data type");
}

} // namespace df

#endif // DF_DS_LIBRARY_TYPES_H
