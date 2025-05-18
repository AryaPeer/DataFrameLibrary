#ifndef DF_DS_LIBRARY_STATS_H
#define DF_DS_LIBRARY_STATS_H

#include "df/types.hpp"
#include <string>

namespace df {

class DataFrame;

namespace stats {

Value mean(const ColumnData& column);
Value sum(const ColumnData& column);
Value max(const ColumnData& column);
Value min(const ColumnData& column);
Value median(const ColumnData& column);
Value var(const ColumnData& column, size_t ddof = 1);
Value std(const ColumnData& column, size_t ddof = 1);
Value count(const ColumnData& column);

DataFrame corr(const DataFrame& df);
DataFrame cov(const DataFrame& df);

} // namespace stats
} // namespace df

#endif // DF_DS_LIBRARY_STATS_H
