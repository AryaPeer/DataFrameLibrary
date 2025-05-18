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

namespace df {

/**
 * Represents a missing/null value
 */
struct NA {
    bool operator==(const NA&) const { return true; }
    bool operator!=(const NA&) const { return false; }
};

// Define a constant NA value for consistency
inline constexpr NA NA_VALUE {};

// Type that can represent a null value for numerical types
template<typename T>
class Nullable {
private:
    std::optional<T> value;

public:
    Nullable() : value(std::nullopt) {}
    Nullable(const T& v) : value(v) {}
    Nullable(NA) : value(std::nullopt) {}
    
    bool is_na() const { return !value.has_value(); }
    T value_or(T default_val) const { return value.value_or(default_val); }
    T value_unsafe() const { return value.value(); }
    
    // Operators with Nullable
    bool operator==(const Nullable& other) const {
        if (is_na() && other.is_na()) return true;
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
    
    // Arithmetic operators
    Nullable operator+(const Nullable& other) const {
        if (is_na() || other.is_na()) return NA_VALUE;
        return Nullable(value.value() + other.value.value());
    }
    
    Nullable operator-(const Nullable& other) const {
        if (is_na() || other.is_na()) return NA_VALUE;
        return Nullable(value.value() - other.value.value());
    }
    
    Nullable operator*(const Nullable& other) const {
        if (is_na() || other.is_na()) return NA_VALUE;
        return Nullable(value.value() * other.value.value());
    }
    
    Nullable operator/(const Nullable& other) const {
        if (is_na() || other.is_na() || other.value.value() == static_cast<T>(0)) return NA_VALUE;
        return Nullable(value.value() / other.value.value());
    }
    
    // Arithmetic operations with raw values
    Nullable operator+(const T& other) const {
        if (is_na()) return NA_VALUE;
        return Nullable(value.value() + other);
    }
    
    Nullable operator-(const T& other) const {
        if (is_na()) return NA_VALUE;
        return Nullable(value.value() - other);
    }
    
    Nullable operator*(const T& other) const {
        if (is_na()) return NA_VALUE;
        return Nullable(value.value() * other);
    }
    
    Nullable operator/(const T& other) const {
        if (is_na() || other == static_cast<T>(0)) return NA_VALUE;
        return Nullable(value.value() / other);
    }
    
    // Compound assignment operators
    Nullable& operator+=(const Nullable& other) {
        if (is_na() || other.is_na()) {
            value = std::nullopt;
        } else {
            value = value.value() + other.value.value();
        }
        return *this;
    }
    
    Nullable& operator-=(const Nullable& other) {
        if (is_na() || other.is_na()) {
            value = std::nullopt;
        } else {
            value = value.value() - other.value.value();
        }
        return *this;
    }
    
    Nullable& operator*=(const Nullable& other) {
        if (is_na() || other.is_na()) {
            value = std::nullopt;
        } else {
            value = value.value() * other.value.value();
        }
        return *this;
    }
    
    Nullable& operator/=(const Nullable& other) {
        if (is_na() || other.is_na() || other.value.value() == static_cast<T>(0)) {
            value = std::nullopt;
        } else {
            value = value.value() / other.value.value();
        }
        return *this;
    }
    
    // Compound assignment with raw values
    Nullable& operator+=(const T& other) {
        if (is_na()) return *this;
        value = value.value() + other;
        return *this;
    }
    
    Nullable& operator-=(const T& other) {
        if (is_na()) return *this;
        value = value.value() - other;
        return *this;
    }
    
    Nullable& operator*=(const T& other) {
        if (is_na()) return *this;
        value = value.value() * other;
        return *this;
    }
    
    Nullable& operator/=(const T& other) {
        if (is_na() || other == static_cast<T>(0)) {
            value = std::nullopt;
        } else {
            value = value.value() / other;
        }
        return *this;
    }
};

// Type aliases for common nullable types
using NullableInt = Nullable<int>;
using NullableDouble = Nullable<double>;
using NullableBool = Nullable<bool>;
using NullableString = std::optional<std::string>; // Uses std::optional directly for string nullability

// Value type that can be stored in a Series
using Value = std::variant<int, double, bool, std::string, NullableInt, NullableDouble, NullableBool, NullableString, NA>;

// Column data type definitions
using IntColumn = std::vector<NullableInt>;
using DoubleColumn = std::vector<NullableDouble>;
using BoolColumn = std::vector<NullableBool>;
using StringColumn = std::vector<NullableString>;

// The column data variant type
using ColumnData = std::variant<IntColumn, DoubleColumn, BoolColumn, StringColumn>;

// Data type enumerations for the DataFrame
enum class DataType {
    Integer,
    Double,
    Boolean,
    String
};

// Mapping from DataType to string representation
inline std::string dataTypeToString(DataType type) {
    switch (type) {
        case DataType::Integer: return "int";
        case DataType::Double: return "double";
        case DataType::Boolean: return "bool";
        case DataType::String: return "string";
        default: return "unknown";
    }
}

// Mapping from string to DataType
inline DataType stringToDataType(const std::string& typeStr) {
    if (typeStr == "int") return DataType::Integer;
    if (typeStr == "double") return DataType::Double;
    if (typeStr == "bool") return DataType::Boolean;
    if (typeStr == "string") return DataType::String;
    throw std::invalid_argument("Unknown data type: " + typeStr);
}

// Function to determine DataType from a ColumnData
inline DataType getDataType(const ColumnData& data) {
    if (std::holds_alternative<IntColumn>(data)) return DataType::Integer;
    if (std::holds_alternative<DoubleColumn>(data)) return DataType::Double;
    if (std::holds_alternative<BoolColumn>(data)) return DataType::Boolean;
    if (std::holds_alternative<StringColumn>(data)) return DataType::String;
    throw std::invalid_argument("Unable to determine data type");
}

} // namespace df

#endif // DF_DS_LIBRARY_TYPES_H
