#include "df/io.hpp"
#include "df/dataframe.hpp"
#include "df/index.hpp"
#include <stdexcept>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <set>

namespace df {

namespace detail {
    struct CSVParseResult {
        std::vector<std::string> headers;
        std::vector<std::pair<std::string, ColumnData>> data;
    };

    static std::vector<std::string> parseCSVLine(const std::string& line, char delimiter = ',', char quotechar = '"') {
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

    static bool hasUnclosedQuote(const std::string& line, char quotechar = '"') {
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

    static bool isNAValue(const std::string& val, const std::vector<std::string>& na_values) {
        for (const auto& na : na_values) {
            if (val == na) return true;
        }
        return false;
    }

    CSVParseResult parse_csv(const std::string& filename, const io::CSVReadOptions& options) {
        std::ifstream file(filename);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + filename);
        }

        CSVParseResult result;
        std::string line;

        // Read header or generate column names
        if (options.header) {
            if (!std::getline(file, line)) {
                return result;
            }
            if (!line.empty() && line.back() == '\r') line.pop_back();
            result.headers = parseCSVLine(line, options.delimiter, options.quotechar);
        } else {
            // Peek at first line to determine column count, then put it back for data processing
            std::streampos startPos = file.tellg();
            if (!std::getline(file, line)) {
                return result;
            }
            if (!line.empty() && line.back() == '\r') line.pop_back();
            auto fields = parseCSVLine(line, options.delimiter, options.quotechar);
            for (size_t i = 0; i < fields.size(); ++i) {
                result.headers.push_back(std::to_string(i));
            }
            // Seek back to re-read as data
            file.seekg(startPos);
        }

        // Skip rows
        for (size_t i = 0; i < options.skiprows; ++i) {
            if (!std::getline(file, line)) break;
        }

        // Read all data rows as strings first
        std::map<std::string, std::vector<std::string>> rawData;
        for (const auto& header : result.headers) {
            rawData[header] = {};
        }

        size_t rowsRead = 0;
        while (std::getline(file, line)) {
            if (options.nrows.has_value() && rowsRead >= options.nrows.value()) {
                break;
            }

            if (!line.empty() && line.back() == '\r') line.pop_back();

            // Handle multi-line quoted fields
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

            // Fill missing columns with empty string
            while (col < result.headers.size()) {
                rawData[result.headers[col]].push_back("");
                col++;
            }

            ++rowsRead;
        }

        // Determine which columns to include
        std::vector<std::string> headersToProcess;
        if (!options.usecols.empty()) {
            std::set<std::string> usecolSet(options.usecols.begin(), options.usecols.end());
            for (const auto& header : result.headers) {
                if (usecolSet.count(header)) {
                    headersToProcess.push_back(header);
                }
            }
        } else {
            headersToProcess = result.headers;
        }

        // Helper: build a column from raw strings using an explicit DataType
        auto buildColumnWithType = [&](const std::string& hdr, DataType dtype,
                                       const std::vector<std::string>& vals) {
            switch (dtype) {
                case DataType::Integer: {
                    IntColumn col;
                    for (const auto& v : vals) {
                        if (v.empty() || isNAValue(v, options.na_values)) {
                            col.push_back(NA_VALUE);
                        } else {
                            int tmp;
                            if (tryParseInt(v, tmp)) {
                                col.push_back(tmp);
                            } else {
                                col.push_back(NA_VALUE);
                            }
                        }
                    }
                    result.data.emplace_back(hdr, col);
                    break;
                }
                case DataType::Double: {
                    DoubleColumn col;
                    for (const auto& v : vals) {
                        if (v.empty() || isNAValue(v, options.na_values)) {
                            col.push_back(NA_VALUE);
                        } else {
                            double tmp;
                            if (tryParseDouble(v, tmp)) {
                                col.push_back(tmp);
                            } else {
                                col.push_back(NA_VALUE);
                            }
                        }
                    }
                    result.data.emplace_back(hdr, col);
                    break;
                }
                case DataType::Boolean: {
                    BoolColumn col;
                    for (const auto& v : vals) {
                        if (v.empty() || isNAValue(v, options.na_values)) {
                            col.push_back(NA_VALUE);
                        } else {
                            std::string lower = v;
                            std::transform(lower.begin(), lower.end(), lower.begin(),
                                           [](unsigned char c) { return std::tolower(c); });
                            col.push_back(lower == "true" || lower == "1");
                        }
                    }
                    result.data.emplace_back(hdr, col);
                    break;
                }
                case DataType::String: {
                    StringColumn col;
                    for (const auto& v : vals) {
                        if (isNAValue(v, options.na_values)) {
                            col.push_back(NA_VALUE);
                        } else {
                            col.push_back(v);
                        }
                    }
                    result.data.emplace_back(hdr, col);
                    break;
                }
            }
        };

        // Type inference per column (or force StringColumn if infer_types is false)
        for (const auto& header : headersToProcess) {
            const auto& values = rawData[header];

            // If dtype is explicitly specified for this column, use it instead of inferring
            auto dtypeIt = options.dtype.find(header);
            if (dtypeIt != options.dtype.end()) {
                buildColumnWithType(header, dtypeIt->second, values);
                continue;
            }

            if (!options.infer_types) {
                // Store everything as StringColumn
                StringColumn col;
                for (const auto& v : values) {
                    if (isNAValue(v, options.na_values)) {
                        col.push_back(NA_VALUE);
                    } else {
                        col.push_back(v);
                    }
                }
                result.data.emplace_back(header, col);
                continue;
            }

            bool allInt = true, allDouble = true, allBool = true;
            bool hasAnyNonNA = false;

            for (const auto& v : values) {
                if (v.empty() || isNAValue(v, options.na_values)) {
                    continue; // NA values are compatible with any type
                }
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
                // All NA — default to DoubleColumn
                DoubleColumn col;
                for (size_t i = 0; i < values.size(); ++i) {
                    col.push_back(NA_VALUE);
                }
                result.data.emplace_back(header, col);
            } else if (allInt) {
                IntColumn col;
                for (const auto& v : values) {
                    if (v.empty() || isNAValue(v, options.na_values)) {
                        col.push_back(NA_VALUE);
                    } else {
                        col.push_back(std::stoi(v));
                    }
                }
                result.data.emplace_back(header, col);
            } else if (allDouble) {
                DoubleColumn col;
                for (const auto& v : values) {
                    if (v.empty() || isNAValue(v, options.na_values)) {
                        col.push_back(NA_VALUE);
                    } else {
                        col.push_back(std::stod(v));
                    }
                }
                result.data.emplace_back(header, col);
            } else if (allBool) {
                BoolColumn col;
                for (const auto& v : values) {
                    if (v.empty() || isNAValue(v, options.na_values)) {
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
                    if (isNAValue(v, options.na_values)) {
                        col.push_back(NA_VALUE);
                    } else {
                        col.push_back(v);
                    }
                }
                result.data.emplace_back(header, col);
            }
        }

        // Update headers to reflect the actual columns we kept
        result.headers = headersToProcess;

        return result;
    }

    void write_csv(const std::string& filename, const DataFrame& df, const io::CSVWriteOptions& options) {
        std::ofstream file(filename);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file for writing: " + filename);
        }

        // Determine which columns to write
        std::vector<std::string> colNames;
        if (!options.columns.empty()) {
            colNames = options.columns;
        } else {
            colNames = df.getColumnNames();
        }

        std::string delim(1, options.delimiter);
        std::string lineEnd(1, options.line_terminator);

        // Write header
        if (options.header) {
            if (options.index) {
                file << delim; // empty header for index column
            }
            for (size_t i = 0; i < colNames.size(); ++i) {
                file << colNames[i];
                if (i < colNames.size() - 1) {
                    file << delim;
                }
            }
            file << lineEnd;
        }

        auto indexPtr = df.getIndex();

        for (size_t row = 0; row < df.numRows(); ++row) {
            // Write index if requested
            if (options.index) {
                file << indexPtr->at(row) << delim;
            }

            for (size_t col = 0; col < colNames.size(); ++col) {
                const auto& colName = colNames[col];
                const auto& colData = df[colName];

                std::visit([&](const auto& vec) {
                    using VecType = std::decay_t<decltype(vec)>;

                    if (row < vec.size()) {
                        if constexpr (std::is_same_v<VecType, IntColumn>) {
                            if (vec[row].is_na()) {
                                file << options.na_rep;
                            } else {
                                file << vec[row].value_unsafe();
                            }
                        }
                        else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
                            if (vec[row].is_na()) {
                                file << options.na_rep;
                            } else {
                                file << vec[row].value_unsafe();
                            }
                        }
                        else if constexpr (std::is_same_v<VecType, BoolColumn>) {
                            if (vec[row].is_na()) {
                                file << options.na_rep;
                            } else {
                                file << (vec[row].value_unsafe() ? "true" : "false");
                            }
                        }
                        else if constexpr (std::is_same_v<VecType, StringColumn>) {
                            if (vec[row].is_na()) {
                                file << options.na_rep;
                            } else {
                                std::string val = vec[row].value_unsafe();
                                bool needsQuoting = options.quote_all ||
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
                    } else {
                        file << options.na_rep;
                    }
                }, colData);

                if (col < colNames.size() - 1) {
                    file << delim;
                }
            }
            file << lineEnd;
        }
    }
}

namespace io {

DataFrame read_csv(const std::string& filename, const CSVReadOptions& options) {
    auto parseResult = detail::parse_csv(filename, options);

    DataFrame df;
    for (const auto& [header, colData] : parseResult.data) {
        df.addColumn(header, colData);
    }

    // Handle index_col
    if (!options.index_col.empty() && df.columnExists(options.index_col)) {
        const auto& colData = df[options.index_col];
        std::vector<std::string> indexLabels;

        std::visit([&](const auto& vec) {
            for (const auto& val : vec) {
                if (val.is_na()) {
                    indexLabels.push_back("NA");
                } else {
                    using VecType = std::decay_t<decltype(vec)>;
                    if constexpr (std::is_same_v<VecType, StringColumn>) {
                        indexLabels.push_back(val.value_unsafe());
                    } else if constexpr (std::is_same_v<VecType, IntColumn>) {
                        indexLabels.push_back(std::to_string(val.value_unsafe()));
                    } else if constexpr (std::is_same_v<VecType, DoubleColumn>) {
                        indexLabels.push_back(std::to_string(val.value_unsafe()));
                    } else if constexpr (std::is_same_v<VecType, BoolColumn>) {
                        indexLabels.push_back(val.value_unsafe() ? "true" : "false");
                    }
                }
            }
        }, colData);

        df.removeColumn(options.index_col);
        if (!indexLabels.empty()) {
            df.setIndex(indexLabels);
        }
    }

    return df;
}

void to_csv(const DataFrame& df, const std::string& filename, const CSVWriteOptions& options) {
    detail::write_csv(filename, df, options);
}

} // namespace io
} // namespace df
