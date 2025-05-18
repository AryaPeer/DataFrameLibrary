#ifndef DF_DS_LIBRARY_TYPES_H
#define DF_DS_LIBRARY_TYPES_H

#include <vector>
#include <string>
#include <variant>
#include <optional>
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

    template<typename U, typename = std::enable_if_t<
        std::is_constructible_v<T, const U&> &&
        !std::is_same_v<std::decay_t<U>, T> &&
        !std::is_same_v<std::decay_t<U>, Nullable> &&
        !std::is_same_v<std::decay_t<U>, NA>>>
    Nullable(const U& v) : value(T(v)) {}

    bool isNA() const { return !value.has_value(); }
    T valueOr(T defaultVal) const { return value.value_or(defaultVal); }
    T valueUnsafe() const { return value.value(); }

    // NA == NA returns false (matches pandas).
    bool operator==(const Nullable& other) const {
        if (isNA() || other.isNA()) return false;
        return value.value() == other.value.value();
    }
    bool operator!=(const Nullable& other) const { return !(*this == other); }

    bool operator==(const T& other) const {
        if (isNA()) return false;
        return value.value() == other;
    }
    bool operator!=(const T& other) const { return !(*this == other); }

    bool operator<(const Nullable& other) const {
        if (isNA() || other.isNA()) return false;
        return value.value() < other.value.value();
    }
    bool operator>(const Nullable& other) const {
        if (isNA() || other.isNA()) return false;
        return value.value() > other.value.value();
    }
    bool operator<=(const Nullable& other) const {
        if (isNA() || other.isNA()) return false;
        return value.value() <= other.value.value();
    }
    bool operator>=(const Nullable& other) const {
        if (isNA() || other.isNA()) return false;
        return value.value() >= other.value.value();
    }

    bool operator<(const T& other) const {
        if (isNA()) return false;
        return value.value() < other;
    }
    bool operator>(const T& other) const {
        if (isNA()) return false;
        return value.value() > other;
    }
    bool operator<=(const T& other) const {
        if (isNA()) return false;
        return value.value() <= other;
    }
    bool operator>=(const T& other) const {
        if (isNA()) return false;
        return value.value() >= other;
    }

    template<typename U = T>
    auto operator+(const Nullable& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (isNA() || other.isNA()) return NA_VALUE;
        return Nullable(value.value() + other.value.value());
    }
    template<typename U = T>
    auto operator-(const Nullable& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (isNA() || other.isNA()) return NA_VALUE;
        return Nullable(value.value() - other.value.value());
    }
    template<typename U = T>
    auto operator*(const Nullable& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (isNA() || other.isNA()) return NA_VALUE;
        return Nullable(value.value() * other.value.value());
    }
    template<typename U = T>
    auto operator/(const Nullable& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (isNA() || other.isNA() || other.value.value() == static_cast<T>(0)) return NA_VALUE;
        return Nullable(value.value() / other.value.value());
    }

    template<typename U = T>
    auto operator+(const T& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (isNA()) return NA_VALUE;
        return Nullable(value.value() + other);
    }
    template<typename U = T>
    auto operator-(const T& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (isNA()) return NA_VALUE;
        return Nullable(value.value() - other);
    }
    template<typename U = T>
    auto operator*(const T& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (isNA()) return NA_VALUE;
        return Nullable(value.value() * other);
    }
    template<typename U = T>
    auto operator/(const T& other) const -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable> {
        if (isNA() || other == static_cast<T>(0)) return NA_VALUE;
        return Nullable(value.value() / other);
    }

    template<typename U = T>
    auto operator+=(const Nullable& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (isNA() || other.isNA()) value = std::nullopt;
        else value = value.value() + other.value.value();
        return *this;
    }
    template<typename U = T>
    auto operator-=(const Nullable& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (isNA() || other.isNA()) value = std::nullopt;
        else value = value.value() - other.value.value();
        return *this;
    }
    template<typename U = T>
    auto operator*=(const Nullable& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (isNA() || other.isNA()) value = std::nullopt;
        else value = value.value() * other.value.value();
        return *this;
    }
    template<typename U = T>
    auto operator/=(const Nullable& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (isNA() || other.isNA() || other.value.value() == static_cast<T>(0)) value = std::nullopt;
        else value = value.value() / other.value.value();
        return *this;
    }

    template<typename U = T>
    auto operator+=(const T& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (isNA()) return *this;
        value = value.value() + other;
        return *this;
    }
    template<typename U = T>
    auto operator-=(const T& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (isNA()) return *this;
        value = value.value() - other;
        return *this;
    }
    template<typename U = T>
    auto operator*=(const T& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (isNA()) return *this;
        value = value.value() * other;
        return *this;
    }
    template<typename U = T>
    auto operator/=(const T& other) -> std::enable_if_t<std::is_arithmetic_v<U>, Nullable&> {
        if (isNA() || other == static_cast<T>(0)) value = std::nullopt;
        else value = value.value() / other;
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

} // namespace df

#endif // DF_DS_LIBRARY_TYPES_H
