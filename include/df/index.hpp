#ifndef DF_DS_LIBRARY_INDEX_H
#define DF_DS_LIBRARY_INDEX_H

#include <vector>
#include <string>
#include <map>
#include <memory>
#include <stdexcept>

namespace df {

/**
 * Index class for DataFrame row identification
 */
class Index {
private:
    std::vector<std::string> labels;
    std::map<std::string, size_t> labelToPos;
    bool isDefaultIndex;

public:
    // Constructors
    Index(size_t size);
    Index(const std::vector<std::string>& labels);
    
    // Copy and move
    Index(const Index& other) = default;
    Index(Index&& other) noexcept = default;
    Index& operator=(const Index& other) = default;
    Index& operator=(Index&& other) noexcept = default;
    
    // Access
    size_t size() const { return labels.size(); }
    const std::string& at(size_t pos) const;
    size_t at(const std::string& label) const;
    bool contains(const std::string& label) const;
    const std::vector<std::string>& getLabels() const { return labels; }
    
    // Manipulation
    Index slice(size_t start, size_t end) const;
    Index take(const std::vector<size_t>& positions) const;
    void append(const std::string& label);
    void extend(const Index& other);
    
    // Predicates
    bool isDefault() const { return isDefaultIndex; }
    
    // Conversion
    std::vector<size_t> toPositions(const std::vector<std::string>& requestedLabels) const;
    
    // Operators
    bool operator==(const Index& other) const;
    bool operator!=(const Index& other) const { return !(*this == other); }
};

} // namespace df

#endif // DF_DS_LIBRARY_INDEX_H
