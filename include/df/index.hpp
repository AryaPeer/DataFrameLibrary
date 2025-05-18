#ifndef DF_DS_LIBRARY_INDEX_H
#define DF_DS_LIBRARY_INDEX_H

#include <vector>
#include <string>
#include <map>

namespace df {

class Index {
private:
    std::vector<std::string> labels;
    std::map<std::string, size_t> labelToPos;
    bool isDefaultIndex;

public:
    Index(size_t size);
    Index(const std::vector<std::string>& labels);

    Index(const Index& other) = default;
    Index(Index&& other) noexcept = default;
    Index& operator=(const Index& other) = default;
    Index& operator=(Index&& other) noexcept = default;

    size_t size() const { return labels.size(); }
    const std::string& at(size_t pos) const;
    size_t at(const std::string& label) const;
    bool contains(const std::string& label) const;
    const std::vector<std::string>& getLabels() const { return labels; }

    Index slice(size_t start, size_t end) const;
    Index take(const std::vector<size_t>& positions) const;

    bool isDefault() const { return isDefaultIndex; }

    bool operator==(const Index& other) const;
    bool operator!=(const Index& other) const { return !(*this == other); }
};

} // namespace df

#endif // DF_DS_LIBRARY_INDEX_H
