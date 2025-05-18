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
    std::string na_values = "NA";
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
    bool index = true;
    bool quote_all = false;
    bool line_terminator = '\n';
    std::vector<std::string> columns = {}; // Empty means all columns
    std::string date_format = "%Y-%m-%d";
};

// CSV I/O functions
DataFrame read_csv(const std::string& filename, const CSVReadOptions& options = CSVReadOptions{});
void to_csv(const DataFrame& df, const std::string& filename, const CSVWriteOptions& options = CSVWriteOptions{});

// JSON I/O functions
DataFrame read_json(const std::string& filename, bool orient_records = true);
void to_json(const DataFrame& df, const std::string& filename, bool orient_records = true);

// Parquet I/O functions
DataFrame read_parquet(const std::string& filename);
void to_parquet(const DataFrame& df, const std::string& filename);

// SQL I/O functions
DataFrame read_sql(const std::string& query, const std::string& connection_string);
void to_sql(const DataFrame& df, const std::string& table_name, const std::string& connection_string, bool if_exists_replace = false);

// Excel I/O functions
DataFrame read_excel(const std::string& filename, const std::string& sheet_name = "Sheet1", size_t header = 0);
void to_excel(const DataFrame& df, const std::string& filename, const std::string& sheet_name = "Sheet1");

// HTML I/O functions
DataFrame read_html(const std::string& path, size_t table_index = 0);
std::string to_html(const DataFrame& df, bool classes = true, bool index = true);

// String conversion
std::string to_string(const DataFrame& df, size_t max_rows = 10, size_t max_cols = 10);
std::string to_string(const ColumnData& column, size_t max_items = 10);

// Clipboard functions
DataFrame read_clipboard();
void to_clipboard(const DataFrame& df);

} // namespace io
} // namespace df

#endif // DF_DS_LIBRARY_IO_H