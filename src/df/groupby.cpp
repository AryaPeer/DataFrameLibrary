#include "df/groupby.hpp"
#include "df/dataframe.hpp"
#include "df/index.hpp"
#include "df/stats.hpp"
#include <algorithm>
#include <stdexcept>
#include <set>

namespace df {

namespace {

Value extractValueAtRow(const ColumnData& col, size_t row) {
    return std::visit([row](const auto& vec) -> Value {
        return vec[row];
    }, col);
}

ColumnData extractSubColumn(const ColumnData& col, const std::vector<size_t>& indices) {
    return std::visit([&](const auto& vec) -> ColumnData {
        using VecType = std::decay_t<decltype(vec)>;
        VecType result;
        result.reserve(indices.size());
        for (size_t idx : indices) {
            result.push_back(vec[idx]);
        }
        return result;
    }, col);
}

ColumnData buildGroupKeyColumn(const ColumnData& sourceCol, const std::vector<Value>& values) {
    return std::visit([&](const auto& srcVec) -> ColumnData {
        using VecType = std::decay_t<decltype(srcVec)>;
        using ElemType = typename VecType::value_type;

        VecType result;
        result.reserve(values.size());
        for (const auto& val : values) {
            if (std::holds_alternative<ElemType>(val)) {
                result.push_back(std::get<ElemType>(val));
            } else {
                result.push_back(NA_VALUE);
            }
        }
        return result;
    }, sourceCol);
}

ColumnData buildAggregatedColumn(const std::vector<Value>& values) {
    bool hasInt = false, hasDouble = false, hasBool = false, hasString = false;

    for (const auto& val : values) {
        if (std::holds_alternative<int>(val))              hasInt = true;
        else if (std::holds_alternative<double>(val))      hasDouble = true;
        else if (std::holds_alternative<bool>(val))        hasBool = true;
        else if (std::holds_alternative<std::string>(val)) hasString = true;
        else if (std::holds_alternative<NullableInt>(val)    && !std::get<NullableInt>(val).isNA())    hasInt = true;
        else if (std::holds_alternative<NullableDouble>(val) && !std::get<NullableDouble>(val).isNA()) hasDouble = true;
        else if (std::holds_alternative<NullableBool>(val)   && !std::get<NullableBool>(val).isNA())   hasBool = true;
        else if (std::holds_alternative<NullableString>(val) && !std::get<NullableString>(val).isNA()) hasString = true;
    }

    if (hasString) {
        StringColumn result;
        result.reserve(values.size());
        for (const auto& val : values) {
            if (std::holds_alternative<std::string>(val))         result.push_back(std::get<std::string>(val));
            else if (std::holds_alternative<NullableString>(val)) result.push_back(std::get<NullableString>(val));
            else                                                  result.push_back(NA_VALUE);
        }
        return result;
    }

    if (hasDouble) {
        DoubleColumn result;
        result.reserve(values.size());
        for (const auto& val : values) {
            if (std::holds_alternative<double>(val)) {
                result.push_back(std::get<double>(val));
            } else if (std::holds_alternative<int>(val)) {
                result.push_back(static_cast<double>(std::get<int>(val)));
            } else if (std::holds_alternative<bool>(val)) {
                result.push_back(std::get<bool>(val) ? 1.0 : 0.0);
            } else if (std::holds_alternative<NullableDouble>(val)) {
                result.push_back(std::get<NullableDouble>(val));
            } else if (std::holds_alternative<NullableInt>(val)) {
                const auto& n = std::get<NullableInt>(val);
                result.push_back(n.isNA() ? NullableDouble(NA_VALUE)
                                          : NullableDouble(static_cast<double>(n.valueUnsafe())));
            } else if (std::holds_alternative<NullableBool>(val)) {
                const auto& n = std::get<NullableBool>(val);
                result.push_back(n.isNA() ? NullableDouble(NA_VALUE)
                                          : NullableDouble(n.valueUnsafe() ? 1.0 : 0.0));
            } else {
                result.push_back(NA_VALUE);
            }
        }
        return result;
    }

    if (hasInt) {
        IntColumn result;
        result.reserve(values.size());
        for (const auto& val : values) {
            if (std::holds_alternative<int>(val)) {
                result.push_back(std::get<int>(val));
            } else if (std::holds_alternative<bool>(val)) {
                result.push_back(std::get<bool>(val) ? 1 : 0);
            } else if (std::holds_alternative<NullableInt>(val)) {
                result.push_back(std::get<NullableInt>(val));
            } else if (std::holds_alternative<NullableBool>(val)) {
                const auto& n = std::get<NullableBool>(val);
                result.push_back(n.isNA() ? NullableInt(NA_VALUE)
                                          : NullableInt(n.valueUnsafe() ? 1 : 0));
            } else {
                result.push_back(NA_VALUE);
            }
        }
        return result;
    }

    if (hasBool) {
        BoolColumn result;
        result.reserve(values.size());
        for (const auto& val : values) {
            if (std::holds_alternative<bool>(val))              result.push_back(std::get<bool>(val));
            else if (std::holds_alternative<NullableBool>(val)) result.push_back(std::get<NullableBool>(val));
            else                                                result.push_back(NA_VALUE);
        }
        return result;
    }

    DoubleColumn allNA(values.size());
    return allNA;
}

} // anonymous namespace


GroupBy::GroupBy(const DataFrame& dataframe, const std::vector<std::string>& byColumns)
    : df(std::make_shared<DataFrame>(dataframe)), by(byColumns)
{
    for (const auto& colName : by) {
        if (!df->columnExists(colName)) {
            throw std::invalid_argument("GroupBy column does not exist: " + colName);
        }
    }

    size_t nRows = df->numRows();
    for (size_t i = 0; i < nRows; ++i) {
        GroupKey key;
        key.reserve(by.size());
        for (const auto& colName : by) {
            key.push_back(extractValueAtRow((*df)[colName], i));
        }
        groups[key].push_back(i);
    }
}

static DataFrame aggregateImpl(
    const DataFrame& df,
    const std::vector<std::string>& by,
    const GroupBy::GroupMap& groups,
    const std::function<Value(const ColumnData&)>& aggFunc)
{
    std::set<std::string> bySet(by.begin(), by.end());
    std::vector<std::string> colNames = df.getColumnNames();
    std::vector<std::string> nonByColumns;
    for (const auto& name : colNames) {
        if (!bySet.count(name)) nonByColumns.push_back(name);
    }

    std::map<std::string, std::vector<Value>> groupKeyValues;
    for (const auto& byCol : by) groupKeyValues[byCol] = {};

    std::map<std::string, std::vector<Value>> aggResults;
    for (const auto& col : nonByColumns) aggResults[col] = {};

    for (const auto& [key, indices] : groups) {
        for (size_t k = 0; k < by.size(); ++k) {
            groupKeyValues[by[k]].push_back(key[k]);
        }
        for (const auto& colName : nonByColumns) {
            ColumnData subCol = extractSubColumn(df[colName], indices);
            aggResults[colName].push_back(aggFunc(subCol));
        }
    }

    std::vector<std::pair<std::string, ColumnData>> resultData;
    for (const auto& byCol : by) {
        resultData.emplace_back(byCol, buildGroupKeyColumn(df[byCol], groupKeyValues[byCol]));
    }
    for (const auto& colName : nonByColumns) {
        resultData.emplace_back(colName, buildAggregatedColumn(aggResults[colName]));
    }

    return DataFrame(resultData);
}

DataFrame GroupBy::count() const {
    return aggregateImpl(*df, by, groups, [](const ColumnData& col) { return stats::count(col); });
}

DataFrame GroupBy::sum() const {
    return aggregateImpl(*df, by, groups, [](const ColumnData& col) { return stats::sum(col); });
}

DataFrame GroupBy::mean() const {
    return aggregateImpl(*df, by, groups, [](const ColumnData& col) { return stats::mean(col); });
}

DataFrame GroupBy::min() const {
    return aggregateImpl(*df, by, groups, [](const ColumnData& col) { return stats::min(col); });
}

DataFrame GroupBy::max() const {
    return aggregateImpl(*df, by, groups, [](const ColumnData& col) { return stats::max(col); });
}

DataFrame GroupBy::median() const {
    return aggregateImpl(*df, by, groups, [](const ColumnData& col) { return stats::median(col); });
}

DataFrame GroupBy::std(size_t ddof) const {
    return aggregateImpl(*df, by, groups,
        [ddof](const ColumnData& col) { return stats::std(col, ddof); });
}

DataFrame GroupBy::var(size_t ddof) const {
    return aggregateImpl(*df, by, groups,
        [ddof](const ColumnData& col) { return stats::var(col, ddof); });
}

DataFrame GroupBy::agg(const std::map<std::string, std::function<Value(const ColumnData&)>>& aggs) const {
    std::map<std::string, std::vector<Value>> groupKeyValues;
    for (const auto& byCol : by) groupKeyValues[byCol] = {};

    std::map<std::string, std::vector<Value>> aggResults;
    for (const auto& [colName, _] : aggs) aggResults[colName] = {};

    for (const auto& [key, indices] : groups) {
        for (size_t k = 0; k < by.size(); ++k) {
            groupKeyValues[by[k]].push_back(key[k]);
        }
        for (const auto& [colName, func] : aggs) {
            if (!df->columnExists(colName)) {
                throw std::invalid_argument("Column does not exist: " + colName);
            }
            ColumnData subCol = extractSubColumn((*df)[colName], indices);
            aggResults[colName].push_back(func(subCol));
        }
    }

    std::vector<std::pair<std::string, ColumnData>> resultData;
    for (const auto& byCol : by) {
        resultData.emplace_back(byCol, buildGroupKeyColumn((*df)[byCol], groupKeyValues[byCol]));
    }
    for (const auto& [colName, _] : aggs) {
        resultData.emplace_back(colName, buildAggregatedColumn(aggResults[colName]));
    }

    return DataFrame(resultData);
}

DataFrame GroupBy::agg(const std::function<Value(const ColumnData&)>& aggFunc) const {
    return aggregateImpl(*df, by, groups, aggFunc);
}

DataFrame GroupBy::transform(const std::function<ColumnData(const ColumnData&)>& func) const {
    std::set<std::string> bySet(by.begin(), by.end());
    std::vector<std::string> colNames = df->getColumnNames();
    size_t nRows = df->numRows();

    std::vector<std::string> nonByColumns;
    for (const auto& name : colNames) {
        if (!bySet.count(name)) nonByColumns.push_back(name);
    }

    std::map<std::string, ColumnData> resultCols;
    for (const auto& colName : nonByColumns) {
        const auto& srcCol = (*df)[colName];
        std::visit([&](const auto& vec) {
            using VecType = std::decay_t<decltype(vec)>;
            resultCols[colName] = VecType(nRows);
        }, srcCol);
    }

    for (const auto& [key, indices] : groups) {
        for (const auto& colName : nonByColumns) {
            ColumnData subCol = extractSubColumn((*df)[colName], indices);
            ColumnData transformed = func(subCol);

            std::visit([&](auto& resultVec) {
                using ResultVecType = std::decay_t<decltype(resultVec)>;
                std::visit([&](const auto& transVec) {
                    using TransVecType = std::decay_t<decltype(transVec)>;
                    if constexpr (std::is_same_v<ResultVecType, TransVecType>) {
                        if (transVec.size() != indices.size()) {
                            throw std::runtime_error("Transform function returned wrong number of rows");
                        }
                        for (size_t i = 0; i < indices.size(); ++i) {
                            resultVec[indices[i]] = transVec[i];
                        }
                    } else {
                        throw std::runtime_error("Transform function returned a different column type");
                    }
                }, transformed);
            }, resultCols[colName]);
        }
    }

    std::vector<std::pair<std::string, ColumnData>> resultData;
    for (const auto& colName : colNames) {
        if (bySet.count(colName)) {
            resultData.emplace_back(colName, (*df)[colName]);
        } else {
            resultData.emplace_back(colName, resultCols[colName]);
        }
    }

    DataFrame result(resultData);
    result.setIndex(df->getIndex().getLabels());
    return result;
}

DataFrame GroupBy::filter(const std::function<bool(const DataFrame&)>& func) const {
    std::vector<size_t> keepIndices;

    for (const auto& [key, indices] : groups) {
        std::vector<std::pair<std::string, ColumnData>> groupData;
        for (const auto& [colName, colData] : df->getColumns()) {
            groupData.emplace_back(colName, extractSubColumn(colData, indices));
        }
        DataFrame groupDF(groupData);
        if (func(groupDF)) {
            keepIndices.insert(keepIndices.end(), indices.begin(), indices.end());
        }
    }

    std::sort(keepIndices.begin(), keepIndices.end());

    std::vector<std::pair<std::string, ColumnData>> resultData;
    for (const auto& [colName, colData] : df->getColumns()) {
        resultData.emplace_back(colName, extractSubColumn(colData, keepIndices));
    }

    DataFrame result(resultData);
    if (!keepIndices.empty()) {
        std::vector<std::string> indexLabels;
        indexLabels.reserve(keepIndices.size());
        const auto& idx = df->getIndex();
        for (size_t i : keepIndices) indexLabels.push_back(idx.at(i));
        result.setIndex(indexLabels);
    }
    return result;
}

std::vector<GroupBy::GroupKey> GroupBy::getGroups() const {
    std::vector<GroupKey> keys;
    keys.reserve(groups.size());
    for (const auto& [key, _] : groups) keys.push_back(key);
    return keys;
}

DataFrame GroupBy::getGroup(const GroupKey& key) const {
    auto it = groups.find(key);
    if (it == groups.end()) {
        throw std::out_of_range("Group key not found");
    }
    const auto& indices = it->second;

    std::vector<std::pair<std::string, ColumnData>> groupData;
    for (const auto& [colName, colData] : df->getColumns()) {
        groupData.emplace_back(colName, extractSubColumn(colData, indices));
    }

    DataFrame result(groupData);
    std::vector<std::string> indexLabels;
    indexLabels.reserve(indices.size());
    const auto& idx = df->getIndex();
    for (size_t i : indices) indexLabels.push_back(idx.at(i));
    result.setIndex(indexLabels);
    return result;
}

} // namespace df
