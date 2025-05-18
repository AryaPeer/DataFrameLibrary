#ifndef DF_DS_LIBRARY_IO_H
#define DF_DS_LIBRARY_IO_H

#include "df/types.hpp"
#include <string>
#include <vector>
#include <map>
#include <optional>

namespace df {

class DataFrame;

namespace io {

struct CSVReadOptions {
    char delimiter = ',';
    char quotechar = '"';
    std::vector<std::string> naValues = {"NA", "na", "N/A", "null", "None"};
    bool header = true;
    bool inferTypes = true;
    size_t skipRows = 0;
    std::optional<size_t> nRows = std::nullopt;
    std::vector<std::string> useCols = {};
    std::map<std::string, DataType> dtype = {};
    std::string indexCol = "";

    // TODO: not implemented yet
    char escapechar = '\\';
    bool parseDates = false;
    std::vector<std::string> dateCols = {};
    std::string dateFormat = "%Y-%m-%d";
    bool lowMemory = true;
};

struct CSVWriteOptions {
    char delimiter = ',';
    char quotechar = '"';
    std::string naRep = "NA";
    bool header = true;
    bool index = false;
    bool quoteAll = false;
    char lineTerminator = '\n';
    std::vector<std::string> columns = {};

    // TODO: not implemented yet
    char escapechar = '\\';
    std::string dateFormat = "%Y-%m-%d";
};

DataFrame readCSV(const std::string& filename, const CSVReadOptions& options = CSVReadOptions{});
void toCSV(const DataFrame& df, const std::string& filename, const CSVWriteOptions& options = CSVWriteOptions{});

} // namespace io
} // namespace df

#endif // DF_DS_LIBRARY_IO_H
