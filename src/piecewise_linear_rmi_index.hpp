#pragma once
#include "learned_index_base.hpp"
#include "regression_utils.hpp"

class PiecewiseLinearRMIIndex : public LearnedRMIIndexBase {
public:
    vector<double> slopes;
    vector<double> intercepts;
    vector<idx_t> segment_bounds;

    PiecewiseLinearRMIIndex(const IndexStorageInfo &info);

    void TrainModel();
    idx_t ChooseSegment(double key) const;
    double PredictPosition(double key) const;
    void LookupKey(double key, vector<row_t>& out) const;

    bool Append(IndexLock&, DataChunk&, Vector&) override;
    bool VerifyAppend(DataChunk&) override;
    bool Query(IndexLock&, DataChunk&, SelectionVector&, vector<row_t>& out);

    void Serialize(Serializer&) const override;
    static unique_ptr<Index> Deserialize(Deserializer&, const IndexStorageInfo&);
};

struct PiecewiseLinearRMIIndexType : public IndexType {
    PiecewiseLinearRMIIndexType();
    unique_ptr<Index> CreateIndex(const IndexStorageInfo&) override;
    unique_ptr<Index> DeserializeIndex(Deserializer&, const IndexStorageInfo&) override;
};
