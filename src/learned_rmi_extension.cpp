#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "linear_rmi_index.hpp"
#include "poly_rmi_index.hpp"
#include "piecewise_linear_rmi_index.hpp"

using namespace duckdb;

extern "C" {

DUCKDB_EXTENSION_API void duckdb_extension_init(DatabaseInstance &db){
    auto &catalog = Catalog::GetSystemCatalog(db);

    catalog.RegisterIndexType(make_unique<LinearRMIIndexType>());
    catalog.RegisterIndexType(make_unique<PolyRMIIndexType>());
    catalog.RegisterIndexType(make_unique<PiecewiseLinearRMIIndexType>());
}

DUCKDB_EXTENSION_API const char *duckdb_extension_version(){
    return DuckDB::LibraryVersion();
}

}
