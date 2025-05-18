#ifndef DF_DS_LIBRARY_IO_H
#define DF_DS_LIBRARY_IO_H

#include "df/types.hpp"
#include <string>
#include <vector>
#include <map>
#include <optional>

namespace df {

// Forward declarations
class DataFrame;

namespace io {

// Options for CSV reading
struct CSVReadOptions {
    char delimiter = ',';
    char quotechar = '"';
    char escapechar = '\\';
    std::vector<std::string> na_values = {"NA", "na", "N/A", "null", "None"};
    bool header = true;
    bool infer_types = true;
    size_t skiprows = 0;
    std::optional<size_t> nrows = std::nullopt;
    std::vector<std::string> usecols = {}; // Empty means all columns
    std::map<std::string, DataType> dtype = {};
    std::string index_col = "";
    bool parse_dates = false;
    std::vector<std::string> date_cols = {};
    std::string date_format = "%Y-%m-%d";
    bool low_memory = true;
};

// Options for CSV writing
struct CSVWriteOptions {
    char delimiter = ',';
    char quotechar = '"';
    char escapechar = '\\';
    std::string na_rep = "NA";
    bool header = true;
    bool index = false;
    bool quote_all = false;
    char line_terminator = '\n';
    std::vector<std::string> columns = {}; // Empty means all columns
    std::string date_format = "%Y-%m-%d";
};

// CSV I/O functions
DataFrame read_csv(const std::string& filename, const CSVReadOptions& options = CSVReadOptions{});
void to_csv(const DataFrame& df, const std::string& filename, const CSVWriteOptions& options = CSVWriteOptions{});

} // namespace io
} // namespace df

#endif // DF_DS_LIBRARY_IO_H