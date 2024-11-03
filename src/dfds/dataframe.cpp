#include "includes/dataframe.h"

DataFrame::DataFrame() {}

DataFrame::~DataFrame() {}

void DataFrame::addIntColumn(const std::string& columnName, const std::vector<int>& data) {
    intColumns[columnName] = data;
}

void DataFrame::addDoubleColumn(const std::string& columnName, const std::vector<double>& data) {
    doubleColumns[columnName] = data;
}

void DataFrame::addStringColumn(const std::string& columnName, const std::vector<std::string>& data) {
    stringColumns[columnName] = data;
}

size_t DataFrame::numRows() const {
    if (!intColumns.empty()) {
        return intColumns.begin()->second.size();
    } else if (!doubleColumns.empty()) {
        return doubleColumns.begin()->second.size();
    } else if (!stringColumns.empty()) {
        return stringColumns.begin()->second.size();
    }
    return 0;
}

size_t DataFrame::numColumns() const {
    return intColumns.size() + doubleColumns.size() + stringColumns.size();
}

bool DataFrame::columnExists(const std::string& columnName) const {
    return intColumns.count(columnName) || doubleColumns.count(columnName) || stringColumns.count(columnName);
}

void DataFrame::removeColumn(const std::string& columnName) {
    intColumns.erase(columnName);
    doubleColumns.erase(columnName);
    stringColumns.erase(columnName);
}
