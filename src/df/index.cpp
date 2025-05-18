#include "df/index.hpp"
#include <stdexcept>
#include <sstream>

namespace df {

Index::Index(size_t size) : isDefaultIndex(true) {
    labels.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        std::string label = std::to_string(i);
        labels.push_back(label);
        labelToPos[label] = i;
    }
}

Index::Index(const std::vector<std::string>& indexLabels) : isDefaultIndex(false) {
    labels.reserve(indexLabels.size());
    for (size_t i = 0; i < indexLabels.size(); ++i) {
        const std::string& label = indexLabels[i];
        if (labelToPos.find(label) != labelToPos.end()) {
            throw std::invalid_argument("Duplicate index label: " + label);
        }
        labels.push_back(label);
        labelToPos[label] = i;
    }
}

const std::string& Index::at(size_t pos) const {
    if (pos >= labels.size()) {
        throw std::out_of_range("Index position out of range: " + std::to_string(pos));
    }
    return labels[pos];
}

size_t Index::at(const std::string& label) const {
    auto it = labelToPos.find(label);
    if (it == labelToPos.end()) {
        throw std::out_of_range("Index label not found: " + label);
    }
    return it->second;
}

bool Index::contains(const std::string& label) const {
    return labelToPos.find(label) != labelToPos.end();
}

Index Index::slice(size_t start, size_t end) const {
    if (start >= end || end > labels.size()) {
        throw std::out_of_range("Invalid index slice range");
    }
    
    std::vector<std::string> slicedLabels;
    slicedLabels.reserve(end - start);
    for (size_t i = start; i < end; ++i) {
        slicedLabels.push_back(labels[i]);
    }
    
    return Index(slicedLabels);
}

Index Index::take(const std::vector<size_t>& positions) const {
    std::vector<std::string> takenLabels;
    takenLabels.reserve(positions.size());
    
    for (size_t pos : positions) {
        if (pos >= labels.size()) {
            throw std::out_of_range("Index position out of range: " + std::to_string(pos));
        }
        takenLabels.push_back(labels[pos]);
    }
    
    return Index(takenLabels);
}

void Index::append(const std::string& label) {
    if (labelToPos.find(label) != labelToPos.end()) {
        throw std::invalid_argument("Duplicate index label: " + label);
    }
    labelToPos[label] = labels.size();
    labels.push_back(label);
}

void Index::extend(const Index& other) {
    labels.reserve(labels.size() + other.labels.size());
    for (const auto& label : other.labels) {
        if (labelToPos.find(label) != labelToPos.end()) {
            throw std::invalid_argument("Duplicate index label: " + label);
        }
        labelToPos[label] = labels.size();
        labels.push_back(label);
    }
}

std::vector<size_t> Index::toPositions(const std::vector<std::string>& requestedLabels) const {
    std::vector<size_t> positions;
    positions.reserve(requestedLabels.size());
    
    for (const auto& label : requestedLabels) {
        auto it = labelToPos.find(label);
        if (it == labelToPos.end()) {
            throw std::out_of_range("Index label not found: " + label);
        }
        positions.push_back(it->second);
    }
    
    return positions;
}

bool Index::operator==(const Index& other) const {
    if (labels.size() != other.labels.size()) {
        return false;
    }
    
    for (size_t i = 0; i < labels.size(); ++i) {
        if (labels[i] != other.labels[i]) {
            return false;
        }
    }
    
    return true;
}

} // namespace df
