#include "duckdb.hpp"
#include "rmi_extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "include/rmi_module.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the RMI index module
	RMIModule::Register(loader);
}

void RmiExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string RmiExtension::Name() {
	return "rmi";
}

string RmiExtension::Version() const {
    return "rmi_extension_v1.0";
}


} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rmi, loader) {
	duckdb::LoadInternal(loader);
}
}