#include "piecewise_linear_rmi_index.hpp"

PiecewiseLinearRMIIndex::PiecewiseLinearRMIIndex(const IndexStorageInfo &info)
    : LearnedRMIIndexBase(info) {}

void PiecewiseLinearRMIIndex::TrainModel(){
    idx_t n=sorted_keys.size();
    if(n==0){
        slopes.clear(); intercepts.clear(); segment_bounds.clear();
        return;
    }

    idx_t k = (idx_t)floor(sqrt((double)n));
    if(k==0) k=1;

    slopes.resize(k);
    intercepts.resize(k);
    segment_bounds.resize(k+1);

    idx_t seg_size = n/k;
    if(seg_size==0) seg_size=1;

    idx_t start=0;
    for(idx_t seg=0; seg<k; seg++){
        idx_t end = (seg==k-1)? n : start+seg_size;
        segment_bounds[seg] = start;

        if(end-start < 2){
            slopes[seg]=0;
            intercepts[seg]=start;
            start=end;
            continue;
        }

        vector<double> x,y;
        for(idx_t i=start;i<end;i++){
            x.push_back(sorted_keys[i]);
            y.push_back((double)i);
        }
        auto ab=FitSimpleLinear(x,y);
        slopes[seg]=ab.first;
        intercepts[seg]=ab.second;

        start=end;
    }
    segment_bounds[k]=n;
}

idx_t PiecewiseLinearRMIIndex::ChooseSegment(double key) const {
    idx_t n = sorted_keys.size();
    auto it = std::lower_bound(sorted_keys.begin(),sorted_keys.end(),key);
    idx_t pos = (idx_t)std::distance(sorted_keys.begin(),it);
    if(pos>=n) pos=n-1;

    auto sit = std::upper_bound(segment_bounds.begin(),
                                segment_bounds.end(), pos);

    if(sit==segment_bounds.begin()) return 0;
    idx_t seg = (idx_t)(sit - segment_bounds.begin() - 1);
    if(seg>=slopes.size()) seg=slopes.size()-1;
    return seg;
}

double PiecewiseLinearRMIIndex::PredictPosition(double key) const {
    if(slopes.empty()) return 0.0;
    idx_t s = ChooseSegment(key);
    return slopes[s]*key + intercepts[s];
}

void PiecewiseLinearRMIIndex::LookupKey(double key, vector<row_t> &out) const {
    idx_t n=sorted_keys.size();
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

bool PiecewiseLinearRMIIndex::Append(IndexLock&, DataChunk &entries, Vector &row_ids){
    vector<double> keys;
    ExtractKeys(entries, keys);

    UnifiedVectorFormat idv;
    row_ids.ToUnifiedFormat(row_ids.size(), idv);
    auto ptr = (row_t*)idv.data;

    vector<KeyRowPair> newpairs;
    for(idx_t i=0;i<entries.size();i++){
        auto idx=idv.sel->get_index(i);
        if(!idv.validity.RowIsValid(idx)) continue;
        newpairs.push_back({keys[i],ptr[idx]});
    }

    vector<KeyRowPair> all;
    for(idx_t i=0;i<sorted_keys.size();i++)
        all.push_back({sorted_keys[i],sorted_rowids[i]});
    all.insert(all.end(), newpairs.begin(), newpairs.end());

    BuildSortedIndex(all);
    TrainModel();
    return true;
}

bool PiecewiseLinearRMIIndex::VerifyAppend(DataChunk&){ return true; }

bool PiecewiseLinearRMIIndex::Query(IndexLock&, DataChunk &keys,
                                    SelectionVector&, vector<row_t>& out){
    UnifiedVectorFormat v;
    idx_t cnt=keys.size();
    keys.data[0].ToUnifiedFormat(cnt,v);

    for(idx_t i=0;i<cnt;i++){
        auto idx=v.sel->get_index(i);
        if(!v.validity.RowIsValid(idx)) continue;

        double k;
        switch(keys.data[0].GetType().id()){
        case LogicalTypeId::BIGINT:
            k=(double)((int64_t*)v.data)[idx];
            break;
        case LogicalTypeId::DOUBLE:
            k=((double*)v.data)[idx];
            break;
        default:
            throw NotImplementedException("PWLinear Query unsupported");
        }

        LookupKey(k, out);
    }
    return true;
}

void PiecewiseLinearRMIIndex::Serialize(Serializer &ser) const {
    ser.Write<uint8_t>((uint8_t)LearnedIndexModelKind::PIECEWISE_LINEAR);
    ser.Write<idx_t>(sorted_keys.size());
    for(idx_t i=0;i<sorted_keys.size();i++){
        ser.Write<double>(sorted_keys[i]);
        ser.Write<row_t>(sorted_rowids[i]);
    }

    ser.Write<idx_t>(slopes.size());
    for(idx_t i=0;i<slopes.size();i++){
        ser.Write<double>(slopes[i]);
        ser.Write<double>(intercepts[i]);
    }

    ser.Write<idx_t>(segment_bounds.size());
    for(auto b: segment_bounds) ser.Write<idx_t>(b);

    ser.Write<idx_t>(window_radius);
}

unique_ptr<Index> PiecewiseLinearRMIIndex::Deserialize(Deserializer &des,
                                        const IndexStorageInfo &info){
    auto idx = make_unique<PiecewiseLinearRMIIndex>(info);

    idx_t n = des.Read<idx_t>();
    idx->sorted_keys.resize(n);
    idx->sorted_rowids.resize(n);

    for(idx_t i=0;i<n;i++){
        idx->sorted_keys[i]=des.Read<double>();
        idx->sorted_rowids[i]=des.Read<row_t>();
    }

    idx_t k = des.Read<idx_t>();
    idx->slopes.resize(k);
    idx->intercepts.resize(k);
    for(idx_t i=0;i<k;i++){
        idx->slopes[i]=des.Read<double>();
        idx->intercepts[i]=des.Read<double>();
    }

    idx_t nb = des.Read<idx_t>();
    idx->segment_bounds.resize(nb);
    for(idx_t i=0;i<nb;i++)
        idx->segment_bounds[i]=des.Read<idx_t>();

    idx->window_radius = des.Read<idx_t>();
    return idx;
}

PiecewiseLinearRMIIndexType::PiecewiseLinearRMIIndexType()
: IndexType("piecewise_linear_rmi") {}

unique_ptr<Index> PiecewiseLinearRMIIndexType::CreateIndex(const IndexStorageInfo &info){
    return make_unique<PiecewiseLinearRMIIndex>(info);
}

unique_ptr<Index> PiecewiseLinearRMIIndexType::DeserializeIndex(
    Deserializer &d, const IndexStorageInfo &info){
    return PiecewiseLinearRMIIndex::Deserialize(d, info);
}
