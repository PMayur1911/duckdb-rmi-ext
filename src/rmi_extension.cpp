#include "rmi_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "rmi/rmi_module.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the HNSW index module
	RMIModule::RegisterIndex(loader);
	RMIModule::RegisterIndexPragmas(loader);
	RMIModule::RegisterIndexScan(loader);
	RMIModule::RegisterIndexPlanScan(loader);
}

void RmiExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string RmiExtension::Name() {
	return "rmi";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(vss, loader) {
	duckdb::LoadInternal(loader);
}
}