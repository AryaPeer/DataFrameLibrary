#ifndef DF_DS_LIBRARY_CSV_PARSER_H
#define DF_DS_LIBRARY_CSV_PARSER_H

#include <string>
#include <vector>
#include <map>
#include <variant>

namespace df {
namespace detail {

struct CsvParseResult {
    std::vector<std::string> columnNames;
    std::map<std::string, std::variant<std::vector<int>, std::vector<double>, std::vector<std::string>, std::vector<bool>>> data;
    size_t rowCount;
};

CsvParseResult parse_csv(const std::string& filename);
void write_csv(const std::string& filename, 
               const std::vector<std::string>& columnNames, 
               const std::map<std::string, std::variant<std::vector<int>, std::vector<double>, std::vector<std::string>, std::vector<bool>>>& data,
               size_t rowCount);

} // namespace detail
} // namespace df

#endif // DF_DS_LIBRARY_CSV_PARSER_H