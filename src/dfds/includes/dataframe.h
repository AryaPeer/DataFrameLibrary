#ifndef DF_DS_LIBRARY_DATAFRAME_H
#define DF_DS_LIBRARY_DATAFRAME_H

#include <vector>
#include <string>
#include <map>
#include <stdexcept>

class DataFrame {
private:
    std::map<std::string, std::vector<int>> intColumns;
    std::map<std::string, std::vector<double>> doubleColumns;
    std::map<std::string, std::vector<std::string>> stringColumns;

public:
    // Constructors
    DataFrame();

    // Destructor
    ~DataFrame();

    // Function to add a new column of integers
    void addIntColumn(const std::string& columnName, const std::vector<int>& data);

    // Function to add a new column of doubles
    void addDoubleColumn(const std::string& columnName, const std::vector<double>& data);

    // Function to add a new column of strings
    void addStringColumn(const std::string& columnName, const std::vector<std::string>& data);

    // Function to get the number of rows in the DataFrame
    size_t numRows() const;

    // Function to get the number of columns in the DataFrame
    size_t numColumns() const;

    // Function to check if a column exists
    bool columnExists(const std::string& columnName) const;

    // Function to remove a column by name
    void removeColumn(const std::string& columnName);
};

#endif  // DF_DS_LIBRARY_DATAFRAME_H
