#include "linear_rmi_index.hpp"

LinearRMIIndex::LinearRMIIndex(const IndexStorageInfo &info)
    : LearnedRMIIndexBase(info) {}

void LinearRMIIndex::TrainModel(){
    idx_t n = sorted_keys.size();
    if(n==0){ slope=0; intercept=0; return; }

    vector<double> x(n), y(n);
    for(idx_t i=0;i<n;i++){
        x[i]=sorted_keys[i];
        y[i]=(double)i;
    }
    auto ab = FitSimpleLinear(x,y);
    slope = ab.first;
    intercept = ab.second;
}

double LinearRMIIndex::PredictPosition(double key) const {
    return slope*key + intercept;
}

void LinearRMIIndex::LookupKey(double key, vector<row_t> &out) const {
    idx_t n = sorted_keys.size();
    if(n==0) return;

    double pred = PredictPosition(key);
    idx_t center = ClampIndex(pred,n);
    idx_t lo = center>window_radius ? center-window_radius : 0;
    idx_t hi = std::min(n-1, center+window_radius);

    for(idx_t i=lo;i<=hi;i++){
        if(sorted_keys[i]==key)
            out.push_back(sorted_rowids[i]);
    }
}

bool LinearRMIIndex::Append(IndexLock&, DataChunk &entries, Vector &row_ids){
    vector<double> keys;
    ExtractKeys(entries, keys);

    UnifiedVectorFormat idv;
    row_ids.ToUnifiedFormat(row_ids.size(), idv);
    auto ptr = (row_t*)idv.data;

    vector<KeyRowPair> newpairs;
    for(idx_t i=0;i<entries.size();i++){
        auto idx=idv.sel->get_index(i);
        if(!idv.validity.RowIsValid(idx)) continue;
        newpairs.push_back({keys[i], ptr[idx]});
    }

    vector<KeyRowPair> all;
    for(idx_t i=0;i<sorted_keys.size();i++)
        all.push_back({sorted_keys[i], sorted_rowids[i]});
    all.insert(all.end(), newpairs.begin(), newpairs.end());

    BuildSortedIndex(all);
    TrainModel();
    return true;
}

bool LinearRMIIndex::VerifyAppend(DataChunk&){ return true; }

bool LinearRMIIndex::Query(IndexLock&, DataChunk &keys,
                           SelectionVector &sel, vector<row_t>& out){
    UnifiedVectorFormat v;
    idx_t cnt = keys.size();
    keys.data[0].ToUnifiedFormat(cnt, v);

    for(idx_t i=0;i<cnt;i++){
        auto idx=v.sel->get_index(i);
        if(!v.validity.RowIsValid(idx)) continue;

        double k;
        switch(keys.data[0].GetType().id()){
        case LogicalTypeId::BIGINT:
            k = (double)((int64_t*)v.data)[idx];
            break;
        case LogicalTypeId::DOUBLE:
            k = ((double*)v.data)[idx];
            break;
        default:
            throw NotImplementedException("LinearRMI Query unsupported type");
        }

        LookupKey(k, out);
    }
    return true;
}

void LinearRMIIndex::Serialize(Serializer &ser) const {
    ser.Write<uint8_t>((uint8_t)LearnedIndexModelKind::LINEAR);
    ser.Write<idx_t>(sorted_keys.size());
    for(idx_t i=0;i<sorted_keys.size();i++){
        ser.Write<double>(sorted_keys[i]);
        ser.Write<row_t>(sorted_rowids[i]);
    }
    ser.Write<double>(slope);
    ser.Write<double>(intercept);
    ser.Write<idx_t>(window_radius);
}

unique_ptr<Index> LinearRMIIndex::Deserialize(Deserializer &des,
                                              const IndexStorageInfo &info){
    auto idx = make_unique<LinearRMIIndex>(info);
    idx_t n = des.Read<idx_t>();
    idx->sorted_keys.resize(n);
    idx->sorted_rowids.resize(n);

    for(idx_t i=0;i<n;i++){
        idx->sorted_keys[i]=des.Read<double>();
        idx->sorted_rowids[i]=des.Read<row_t>();
    }
    idx->slope = des.Read<double>();
    idx->intercept = des.Read<double>();
    idx->window_radius = des.Read<idx_t>();
    return idx;
}

LinearRMIIndexType::LinearRMIIndexType()
: IndexType("linear_rmi") {}

unique_ptr<Index> LinearRMIIndexType::CreateIndex(const IndexStorageInfo &info){
    return make_unique<LinearRMIIndex>(info);
}

unique_ptr<Index> LinearRMIIndexType::DeserializeIndex(
    Deserializer &d, const IndexStorageInfo &info){
    return LinearRMIIndex::Deserialize(d, info);
}
