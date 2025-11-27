#pragma once
#include "learned_index_base.hpp"
#include "regression_utils.hpp"

class LinearRMIIndex : public LearnedRMIIndexBase {
public:
    double slope = 0.0;
    double intercept = 0.0;

    LinearRMIIndex(const IndexStorageInfo &info);

    void TrainModel();
    double PredictPosition(double key) const;
    void LookupKey(double key, vector<row_t> &out) const;

    bool Append(IndexLock&, DataChunk&, Vector&) override;
    bool VerifyAppend(DataChunk&) override;
    bool Query(IndexLock&, DataChunk&, SelectionVector&, vector<row_t>& out);

    void Serialize(Serializer&) const override;
    static unique_ptr<Index> Deserialize(Deserializer&, const IndexStorageInfo &info);
};

struct LinearRMIIndexType : public IndexType {
    LinearRMIIndexType();
    unique_ptr<Index> CreateIndex(const IndexStorageInfo &info) override;
    unique_ptr<Index> DeserializeIndex(Deserializer&, const IndexStorageInfo&) override;
};
