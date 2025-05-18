#include "df/io.hpp"
#include "df/dataframe.hpp"
#include "df/index.hpp"
#include <stdexcept>
#include <fstream>
#include <vector>
#include <algorithm>
#include <set>

namespace df {

namespace detail {

struct CSVParseResult {
    std::vector<std::string> headers;
    std::vector<std::pair<std::string, ColumnData>> data;
};

static std::vector<std::string> parseCSVLine(const std::string& line, char delimiter, char quotechar) {
    std::vector<std::string> fields;
    std::string field;
    bool inQuotes = false;

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (inQuotes) {
            if (c == quotechar) {
                if (i + 1 < line.size() && line[i + 1] == quotechar) {
                    field += quotechar;
                    ++i;
                } else {
                    inQuotes = false;
                }
            } else {
                field += c;
            }
        } else {
            if (c == quotechar) {
                inQuotes = true;
            } else if (c == delimiter) {
                fields.push_back(field);
                field.clear();
            } else {
                field += c;
            }
        }
    }
    fields.push_back(field);
    return fields;
}

static bool hasUnclosedQuote(const std::string& line, char quotechar) {
    bool inQuotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        if (line[i] == quotechar) {
            if (inQuotes && i + 1 < line.size() && line[i + 1] == quotechar) {
                ++i;
            } else {
                inQuotes = !inQuotes;
            }
        }
    }
    return inQuotes;
}

static bool tryParseInt(const std::string& s, int& out) {
    if (s.empty()) return false;
    try {
        size_t pos;
        out = std::stoi(s, &pos);
        return pos == s.size();
    } catch (...) {
        return false;
    }
}

static bool tryParseDouble(const std::string& s, double& out) {
    if (s.empty()) return false;
    try {
        size_t pos;
        out = std::stod(s, &pos);
        return pos == s.size();
    } catch (...) {
        return false;
    }
}

static bool isNAToken(const std::string& val, const std::vector<std::string>& naValues) {
    for (const auto& na : naValues) {
        if (val == na) return true;
    }
    return false;
}

CSVParseResult parseCSV(const std::string& filename, const io::CSVReadOptions& options) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filename);
    }

    CSVParseResult result;
    std::string line;

    if (options.header) {
        if (!std::getline(file, line)) return result;
        if (!line.empty() && line.back() == '\r') line.pop_back();
        result.headers = parseCSVLine(line, options.delimiter, options.quotechar);
    } else {
        std::streampos startPos = file.tellg();
        if (!std::getline(file, line)) return result;
        if (!line.empty() && line.back() == '\r') line.pop_back();
        auto fields = parseCSVLine(line, options.delimiter, options.quotechar);
        for (size_t i = 0; i < fields.size(); ++i) {
            result.headers.push_back(std::to_string(i));
        }
        file.seekg(startPos);
    }

    for (size_t i = 0; i < options.skipRows; ++i) {
        if (!std::getline(file, line)) break;
    }

    std::map<std::string, std::vector<std::string>> rawData;
    for (const auto& header : result.headers) rawData[header] = {};

    size_t rowsRead = 0;
    while (std::getline(file, line)) {
        if (options.nRows.has_value() && rowsRead >= options.nRows.value()) break;
        if (!line.empty() && line.back() == '\r') line.pop_back();

        std::string nextLine;
        while (hasUnclosedQuote(line, options.quotechar) && std::getline(file, nextLine)) {
            if (!nextLine.empty() && nextLine.back() == '\r') nextLine.pop_back();
            line += '\n' + nextLine;
        }

        auto fields = parseCSVLine(line, options.delimiter, options.quotechar);
        size_t col = 0;
        for (; col < fields.size() && col < result.headers.size(); ++col) {
            rawData[result.headers[col]].push_back(fields[col]);
        }
        while (col < result.headers.size()) {
            rawData[result.headers[col]].push_back("");
            col++;
        }
        ++rowsRead;
    }

    std::vector<std::string> headersToProcess;
    if (!options.useCols.empty()) {
        std::set<std::string> useColSet(options.useCols.begin(), options.useCols.end());
        for (const auto& header : result.headers) {
            if (useColSet.count(header)) headersToProcess.push_back(header);
        }
    } else {
        headersToProcess = result.headers;
    }

    auto buildColumnWithType = [&](const std::string& hdr, DataType dtype,
                                   const std::vector<std::string>& vals) {
        switch (dtype) {
            case DataType::Integer: {
                IntColumn col;
                for (const auto& v : vals) {
                    if (v.empty() || isNAToken(v, options.naValues)) {
                        col.push_back(NA_VALUE);
                    } else {
                        int tmp;
                        col.push_back(tryParseInt(v, tmp) ? NullableInt(tmp) : NullableInt(NA_VALUE));
                    }
                }
                result.data.emplace_back(hdr, col);
                break;
            }
            case DataType::Double: {
                DoubleColumn col;
                for (const auto& v : vals) {
                    if (v.empty() || isNAToken(v, options.naValues)) {
                        col.push_back(NA_VALUE);
                    } else {
                        double tmp;
                        col.push_back(tryParseDouble(v, tmp) ? NullableDouble(tmp) : NullableDouble(NA_VALUE));
                    }
                }
                result.data.emplace_back(hdr, col);
                break;
            }
            case DataType::Boolean: {
                BoolColumn col;
                for (const auto& v : vals) {
                    if (v.empty() || isNAToken(v, options.naValues)) {
                        col.push_back(NA_VALUE);
                    } else {
                        std::string lower = v;
                        std::transform(lower.begin(), lower.end(), lower.begin(),
                                       [](unsigned char c) { return std::tolower(c); });
                        if (lower == "true" || lower == "1")       col.push_back(true);
                        else if (lower == "false" || lower == "0") col.push_back(false);
                        else                                       col.push_back(NA_VALUE);
                    }
                }
                result.data.emplace_back(hdr, col);
                break;
            }
            case DataType::String: {
                StringColumn col;
                for (const auto& v : vals) {
                    if (isNAToken(v, options.naValues)) col.push_back(NA_VALUE);
                    else col.push_back(v);
                }
                result.data.emplace_back(hdr, col);
                break;
            }
        }
    };

    for (const auto& header : headersToProcess) {
        const auto& values = rawData[header];

        auto dtypeIt = options.dtype.find(header);
        if (dtypeIt != options.dtype.end()) {
            buildColumnWithType(header, dtypeIt->second, values);
            continue;
        }

        if (!options.inferTypes) {
            StringColumn col;
            for (const auto& v : values) {
                if (isNAToken(v, options.naValues)) col.push_back(NA_VALUE);
                else col.push_back(v);
            }
            result.data.emplace_back(header, col);
            continue;
        }

        bool allInt = true, allDouble = true, allBool = true;
        bool hasAnyNonNA = false;

        for (const auto& v : values) {
            if (v.empty() || isNAToken(v, options.naValues)) continue;
            hasAnyNonNA = true;

            int tmpI; double tmpD;
            if (!tryParseInt(v, tmpI)) allInt = false;
            if (!tryParseDouble(v, tmpD)) allDouble = false;

            std::string lower = v;
            std::transform(lower.begin(), lower.end(), lower.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            if (lower != "true" && lower != "false") allBool = false;
        }

        if (!hasAnyNonNA) {
            DoubleColumn col(values.size());
            result.data.emplace_back(header, col);
        } else if (allInt) {
            IntColumn col;
            for (const auto& v : values) {
                if (v.empty() || isNAToken(v, options.naValues)) col.push_back(NA_VALUE);
                else col.push_back(std::stoi(v));
            }
            result.data.emplace_back(header, col);
        } else if (allDouble) {
            DoubleColumn col;
            for (const auto& v : values) {
                if (v.empty() || isNAToken(v, options.naValues)) col.push_back(NA_VALUE);
                else col.push_back(std::stod(v));
            }
            result.data.emplace_back(header, col);
        } else if (allBool) {
            BoolColumn col;
            for (const auto& v : values) {
                if (v.empty() || isNAToken(v, options.naValues)) {
                    col.push_back(NA_VALUE);
                } else {
                    std::string lower = v;
                    std::transform(lower.begin(), lower.end(), lower.begin(),
                                   [](unsigned char c) { return std::tolower(c); });
                    col.push_back(lower == "true");
                }
            }
            result.data.emplace_back(header, col);
        } else {
            StringColumn col;
            for (const auto& v : values) {
                if (isNAToken(v, options.naValues)) col.push_back(NA_VALUE);
                else col.push_back(v);
            }
            result.data.emplace_back(header, col);
        }
    }

    result.headers = headersToProcess;
    return result;
}

void writeCSV(const std::string& filename, const DataFrame& df, const io::CSVWriteOptions& options) {
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }

    std::vector<std::string> colNames = options.columns.empty() ? df.getColumnNames() : options.columns;

    std::string delim(1, options.delimiter);
    std::string lineEnd(1, options.lineTerminator);

    if (options.header) {
        if (options.index) file << delim;
        for (size_t i = 0; i < colNames.size(); ++i) {
            file << colNames[i];
            if (i < colNames.size() - 1) file << delim;
        }
        file << lineEnd;
    }

    const Index& idx = df.getIndex();

    for (size_t row = 0; row < df.numRows(); ++row) {
        if (options.index) file << idx.at(row) << delim;

        for (size_t col = 0; col < colNames.size(); ++col) {
            const auto& colName = colNames[col];
            const auto& colData = df[colName];

            std::visit([&](const auto& vec) {
                using VecType = std::decay_t<decltype(vec)>;
                if (row >= vec.size()) { file << options.naRep; return; }

                if constexpr (std::is_same_v<VecType, IntColumn> ||
                              std::is_same_v<VecType, DoubleColumn>) {
                    if (vec[row].isNA()) file << options.naRep;
                    else file << vec[row].valueUnsafe();
                }
                else if constexpr (std::is_same_v<VecType, BoolColumn>) {
                    if (vec[row].isNA()) file << options.naRep;
                    else file << (vec[row].valueUnsafe() ? "true" : "false");
                }
                else if constexpr (std::is_same_v<VecType, StringColumn>) {
                    if (vec[row].isNA()) {
                        file << options.naRep;
                    } else {
                        std::string val = vec[row].valueUnsafe();
                        bool needsQuoting = options.quoteAll ||
                            val.find(options.delimiter) != std::string::npos ||
                            val.find(options.quotechar) != std::string::npos ||
                            val.find('\n') != std::string::npos;
                        if (needsQuoting) {
                            std::string escaped = val;
                            std::string qc(1, options.quotechar);
                            size_t pos = 0;
                            while ((pos = escaped.find(options.quotechar, pos)) != std::string::npos) {
                                escaped.insert(pos, qc);
                                pos += 2;
                            }
                            file << options.quotechar << escaped << options.quotechar;
                        } else {
                            file << val;
                        }
                    }
                }
            }, colData);

            if (col < colNames.size() - 1) file << delim;
        }
        file << lineEnd;
    }
}

} // namespace detail

namespace io {

DataFrame readCSV(const std::string& filename, const CSVReadOptions& options) {
    auto parseResult = detail::parseCSV(filename, options);

    DataFrame df;
    for (const auto& [header, colData] : parseResult.data) {
        df.addColumn(header, colData);
    }

    if (!options.indexCol.empty() && df.columnExists(options.indexCol)) {
        const auto& colData = df[options.indexCol];
        std::vector<std::string> indexLabels;

        std::visit([&](const auto& vec) {
            using VecType = std::decay_t<decltype(vec)>;
            for (const auto& val : vec) {
                if (val.isNA()) {
                    indexLabels.push_back("NA");
                } else if constexpr (std::is_same_v<VecType, StringColumn>) {
                    indexLabels.push_back(val.valueUnsafe());
                } else if constexpr (std::is_same_v<VecType, IntColumn> ||
                                     std::is_same_v<VecType, DoubleColumn>) {
                    indexLabels.push_back(std::to_string(val.valueUnsafe()));
                } else if constexpr (std::is_same_v<VecType, BoolColumn>) {
                    indexLabels.push_back(val.valueUnsafe() ? "true" : "false");
                }
            }
        }, colData);

        df.removeColumn(options.indexCol);
        if (!indexLabels.empty()) df.setIndex(indexLabels);
    }

    return df;
}

void toCSV(const DataFrame& df, const std::string& filename, const CSVWriteOptions& options) {
    detail::writeCSV(filename, df, options);
}

} // namespace io
} // namespace df
