#include "learned_index_base.hpp"
#include <algorithm>

LearnedRMIIndexBase::LearnedRMIIndexBase(const IndexStorageInfo &info)
    : Index(info) {}

void LearnedRMIIndexBase::BuildSortedIndex(vector<KeyRowPair> &pairs) {
    std::sort(pairs.begin(), pairs.end(),
              [](auto &a, auto &b){ return a.key < b.key; });

    sorted_keys.clear();
    sorted_rowids.clear();

    sorted_keys.reserve(pairs.size());
    sorted_rowids.reserve(pairs.size());

    for (auto &p : pairs) {
        sorted_keys.push_back(p.key);
        sorted_rowids.push_back(p.rowid);
    }
}

void LearnedRMIIndexBase::ExtractKeys(const DataChunk &entries,
                                      vector<double> &keys_out) const {
    auto &col = entries.data[0];
    idx_t cnt = entries.size();

    switch (col.GetType().id()) {
    case LogicalTypeId::BIGINT: {
        UnifiedVectorFormat v;
        col.ToUnifiedFormat(cnt, v);
        auto ptr = (int64_t *)v.data;
        for(idx_t i=0;i<cnt;i++){
            auto idx=v.sel->get_index(i);
            if(!v.validity.RowIsValid(idx)) continue;
            keys_out.push_back((double)ptr[idx]);
        }
        break;
    }
    case LogicalTypeId::DOUBLE: {
        UnifiedVectorFormat v;
        col.ToUnifiedFormat(cnt, v);
        auto ptr = (double *)v.data;
        for(idx_t i=0;i<cnt;i++){
            auto idx=v.sel->get_index(i);
            if(!v.validity.RowIsValid(idx)) continue;
            keys_out.push_back(ptr[idx]);
        }
        break;
    }
    default:
        throw NotImplementedException("Unsupported type for learned index");
    }
}
