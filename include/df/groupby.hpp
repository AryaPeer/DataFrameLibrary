#ifndef DF_DS_LIBRARY_GROUPBY_H
#define DF_DS_LIBRARY_GROUPBY_H

#include "df/types.hpp"
#include <vector>
#include <string>
#include <map>
#include <functional>
#include <memory>

namespace df {

class DataFrame;

/**
 * GroupBy class for pandas-like grouping operations
 */
class GroupBy {
public:
    using GroupKey = std::vector<Value>;
    using GroupMap = std::map<GroupKey, std::vector<size_t>>;

private:
    std::shared_ptr<DataFrame> df;
    std::vector<std::string> by;
    GroupMap groups;

public:
    // Constructor
    GroupBy(const DataFrame& dataframe, const std::vector<std::string>& by_columns);
    
    // Aggregation methods
    DataFrame count() const;
    DataFrame sum() const;
    DataFrame mean() const;
    DataFrame min() const;
    DataFrame max() const;
    DataFrame median() const;
    DataFrame std(size_t ddof = 1) const;
    DataFrame var(size_t ddof = 1) const;
    
    // Custom aggregations
    DataFrame agg(const std::map<std::string, std::function<Value(const ColumnData&)>>& aggs) const;
    DataFrame agg(const std::function<Value(const ColumnData&)>& agg) const;
    
    // Transformation
    DataFrame transform(const std::function<ColumnData(const ColumnData&)>& func) const;
    
    // Filtering
    DataFrame filter(const std::function<bool(const DataFrame&)>& func) const;
    
    // Access to groups
    std::vector<GroupKey> getGroups() const;
    DataFrame getGroup(const GroupKey& key) const;
    
    // Information
    size_t size() const { return groups.size(); }
};

} // namespace df

#endif // DF_DS_LIBRARY_GROUPBY_H
