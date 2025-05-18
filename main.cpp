#include "df/dataframe.hpp"
#include "df/io.hpp"
#include <iostream>

static void printHeader(const std::string& title) {
    std::cout << "\n" << title << "\n" << std::string(title.length(), '=') << std::endl;
}

template<typename T>
static void printValueOrNA(const df::Value& v) {
    if (std::holds_alternative<T>(v)) std::cout << std::get<T>(v) << std::endl;
    else std::cout << "N/A" << std::endl;
}

int main() {
    printHeader("DataFrame Creation and Display Test");

    df::IntColumn intCol       = {1, 2, 3, 4, 5};
    df::DoubleColumn doubleCol = {1.1, 2.2, 3.3, 4.4, 5.5};
    df::StringColumn stringCol = {"alpha", "beta", "gamma", "delta", "epsilon"};
    df::BoolColumn boolCol     = {true, false, true, false, true};

    intCol[2]    = df::NA_VALUE;
    doubleCol[3] = df::NA_VALUE;
    stringCol[1] = df::NA_VALUE;

    df::DataFrame dataframe({
        {"integers", intCol},
        {"doubles",  doubleCol},
        {"strings",  stringCol},
        {"booleans", boolCol},
    });

    std::cout << "Full DataFrame:" << std::endl;
    dataframe.display();

    printHeader("Basic Column Operations");

    std::cout << "Number of columns: " << dataframe.numColumns() << std::endl;
    std::cout << "Number of rows: "    << dataframe.numRows() << std::endl;
    std::cout << "Column names: ";
    for (const auto& name : dataframe.getColumnNames()) std::cout << name << " ";
    std::cout << std::endl;

    std::cout << "Column 'integers' exists: "    << (dataframe.columnExists("integers")    ? "yes" : "no") << std::endl;
    std::cout << "Column 'nonexistent' exists: " << (dataframe.columnExists("nonexistent") ? "yes" : "no") << std::endl;

    printHeader("Statistical Functions Test");

    std::cout << "Mean of integers: ";
    printValueOrNA<double>(dataframe.mean("integers"));

    std::cout << "Sum of integers: ";
    df::Value sumVal = dataframe.sum("integers");
    if (std::holds_alternative<int>(sumVal))         std::cout << std::get<int>(sumVal) << std::endl;
    else if (std::holds_alternative<double>(sumVal)) std::cout << std::get<double>(sumVal) << std::endl;
    else                                             std::cout << "N/A" << std::endl;

    std::cout << "Min of doubles: ";                 printValueOrNA<double>(dataframe.min("doubles"));
    std::cout << "Max of doubles: ";                 printValueOrNA<double>(dataframe.max("doubles"));
    std::cout << "Standard deviation of doubles: ";  printValueOrNA<double>(dataframe.std("doubles"));

    printHeader("Row Slicing Test");

    std::cout << "First 3 rows:" << std::endl;
    dataframe.head(3).display();
    std::cout << "Last 2 rows:" << std::endl;
    dataframe.tail(2).display();
    std::cout << "Rows 1-3:" << std::endl;
    dataframe(1, 4).display();

    printHeader("Column Selection Test");

    std::cout << "Selected columns (integers, booleans):" << std::endl;
    dataframe.select({"integers", "booleans"}).display();

    printHeader("Math Operations Test");

    std::cout << "Adding 10 to each numeric column:" << std::endl;
    dataframe.add(10).display();

    printHeader("Create a second DataFrame for binary operations");

    df::DataFrame df2({
        {"integers", df::IntColumn{10, 20, 30, 40, 50}},
        {"doubles",  df::DoubleColumn{0.1, 0.2, 0.3, 0.4, 0.5}},
    });
    std::cout << "Second DataFrame:" << std::endl;
    df2.display();

    std::cout << "Adding the two DataFrames:" << std::endl;
    dataframe.add(df2).display();

    std::cout << "Multiplying the two DataFrames:" << std::endl;
    dataframe.mul(df2).display();

    printHeader("CSV I/O Test");

    const std::string testFile = "test_dataframe.csv";
    std::cout << "Saving DataFrame to " << testFile << std::endl;
    dataframe.toCSV(testFile);
    std::cout << "Data written to CSV file successfully" << std::endl;

    std::cout << "Reading CSV back from " << testFile << std::endl;
    df::DataFrame readBack = df::DataFrame::readCSV(testFile);
    std::cout << "Read-back DataFrame:" << std::endl;
    readBack.display();

    if (readBack.numColumns() == dataframe.numColumns()) {
        std::cout << "Column count matches: " << readBack.numColumns() << std::endl;
    } else {
        std::cout << "ERROR: Column count mismatch! Expected " << dataframe.numColumns()
                  << " but got " << readBack.numColumns() << std::endl;
    }

    if (readBack.numRows() == dataframe.numRows()) {
        std::cout << "Row count matches: " << readBack.numRows() << std::endl;
    } else {
        std::cout << "ERROR: Row count mismatch! Expected " << dataframe.numRows()
                  << " but got " << readBack.numRows() << std::endl;
    }

    printHeader("Basic DataFrame Info");
    dataframe.info();

    return 0;
}
