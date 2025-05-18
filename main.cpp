#include "df/dataframe.hpp"
#include "df/math.hpp"
#include "df/stats.hpp"
#include "df/io.hpp"
#include <iostream>
#include <iomanip>
#include <fstream>

void printSeparator() {
    std::cout << "\n" << std::string(50, '-') << "\n" << std::endl;
}

void printHeader(const std::string& title) {
    std::cout << "\n" << title << "\n" << std::string(title.length(), '=') << std::endl;
}

int main() {
    printHeader("DataFrame Creation and Display Test");
    
    df::IntColumn int_col = {1, 2, 3, 4, 5};
    df::DoubleColumn double_col = {1.1, 2.2, 3.3, 4.4, 5.5};
    df::StringColumn string_col = {"alpha", "beta", "gamma", "delta", "epsilon"};
    df::BoolColumn bool_col = {true, false, true, false, true};
    
    int_col[2] = df::NA_VALUE;
    double_col[3] = df::NA_VALUE;
    string_col[1] = std::nullopt;
    
    std::map<std::string, df::ColumnData> data = {
        {"integers", int_col},
        {"doubles", double_col},
        {"strings", string_col},
        {"booleans", bool_col}
    };
    
    df::DataFrame dataframe(data);
    
    std::cout << "Full DataFrame:" << std::endl;
    dataframe.display();
    
    printHeader("Basic Column Operations");
    
    std::cout << "Number of columns: " << dataframe.numColumns() << std::endl;
    std::cout << "Number of rows: " << dataframe.numRows() << std::endl;
    std::cout << "Column names: ";
    for (const auto& name : dataframe.getColumnNames()) {
        std::cout << name << " ";
    }
    std::cout << std::endl;
    
    std::cout << "Column 'integers' exists: " << (dataframe.columnExists("integers") ? "yes" : "no") << std::endl;
    std::cout << "Column 'nonexistent' exists: " << (dataframe.columnExists("nonexistent") ? "yes" : "no") << std::endl;
    
    printHeader("Statistical Functions Test");
    
    std::cout << "Mean of integers: ";
    df::Value mean_val = df::stats::mean(dataframe, "integers");
    if (std::holds_alternative<double>(mean_val)) {
        std::cout << std::get<double>(mean_val) << std::endl;
    } else {
        std::cout << "N/A" << std::endl;
    }
    
    std::cout << "Sum of integers: ";
    df::Value sum_val = df::stats::sum(dataframe, "integers");
    if (std::holds_alternative<int>(sum_val)) {
        std::cout << std::get<int>(sum_val) << std::endl;
    } else if (std::holds_alternative<double>(sum_val)) {
        std::cout << std::get<double>(sum_val) << std::endl;
    } else {
        std::cout << "N/A" << std::endl;
    }
    
    std::cout << "Min of doubles: ";
    df::Value min_val = df::stats::min(dataframe, "doubles");
    if (std::holds_alternative<double>(min_val)) {
        std::cout << std::get<double>(min_val) << std::endl;
    } else {
        std::cout << "N/A" << std::endl;
    }
    
    std::cout << "Max of doubles: ";
    df::Value max_val = df::stats::max(dataframe, "doubles");
    if (std::holds_alternative<double>(max_val)) {
        std::cout << std::get<double>(max_val) << std::endl;
    } else {
        std::cout << "N/A" << std::endl;
    }
    
    std::cout << "Standard deviation of doubles: ";
    df::Value std_val = df::stats::std(dataframe, "doubles");
    if (std::holds_alternative<double>(std_val)) {
        std::cout << std::get<double>(std_val) << std::endl;
    } else {
        std::cout << "N/A" << std::endl;
    }
    
    printHeader("Row Slicing Test");
    
    std::cout << "First 3 rows:" << std::endl;
    df::DataFrame head = dataframe.head(3);
    head.display();
    
    std::cout << "Last 2 rows:" << std::endl;
    df::DataFrame tail = dataframe.tail(2);
    tail.display();
    
    std::cout << "Rows 1-3:" << std::endl;
    df::DataFrame slice = dataframe(1, 4);  
    slice.display();
    
    printHeader("Column Selection Test");
    
    std::vector<std::string> selected_cols = {"integers", "booleans"};
    df::DataFrame selected_df = dataframe.select(selected_cols);
    std::cout << "Selected columns (integers, booleans):" << std::endl;
    selected_df.display();
    
    printHeader("Math Operations Test");
    
    std::cout << "Adding 10 to each numeric column:" << std::endl;
    df::DataFrame added_df = df::math::add(dataframe, 10);
    added_df.display();
    
    printHeader("Create a second DataFrame for binary operations");
    
    df::IntColumn int_col2 = {10, 20, 30, 40, 50};
    df::DoubleColumn double_col2 = {0.1, 0.2, 0.3, 0.4, 0.5};
    
    std::map<std::string, df::ColumnData> data2 = {
        {"integers", int_col2},
        {"doubles", double_col2}
    };
    
    df::DataFrame df2(data2);
    std::cout << "Second DataFrame:" << std::endl;
    df2.display();
    
    std::cout << "Adding the two DataFrames:" << std::endl;
    df::DataFrame df_sum = df::math::add(dataframe, df2);
    df_sum.display();
    
    std::cout << "Multiplying the two DataFrames:" << std::endl;
    df::DataFrame df_product = df::math::multiply(dataframe, df2);
    df_product.display();
    
    printHeader("CSV I/O Test");
    
    std::string test_file = "test_dataframe.csv";
    std::cout << "Saving DataFrame to " << test_file << std::endl;
    
    df::io::to_csv(dataframe, test_file);
    std::cout << "Data written to CSV file successfully" << std::endl;
      std::cout << "CSV reading skipped in this test" << std::endl;
    
    printHeader("Basic DataFrame Info");
    
    dataframe.info();
    
    return 0;
}
