#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

struct RMIModule {
public:
    static void Register(ExtensionLoader &loader) {
        auto &db = loader.GetDatabaseInstance();

        RegisterIndex(db);
        RegisterIndexScan(loader);
        RegisterIndexPragmas(loader);
        // RegisterMacros(loader);
        
        // Optimizers
        RegisterScanOptimizer(db);
		// RegisterExprOptimizer(db);
		// RegisterTopKOptimizer(db);
		// RegisterJoinOptimizer(db);
    }

private:
    // Registers RMIIndex as a new index type
    static void RegisterIndex(DatabaseInstance &db);

    // Registers the RMI index scan table function
    static void RegisterIndexScan(ExtensionLoader &loader);

    // Registers PRAGMA functions such as PRAGMA rmi_index_info();
    static void RegisterIndexPragmas(ExtensionLoader &loader);
	
    // static void RegisterMacros(ExtensionLoader &loader);


    // Optimizers
    static void RegisterScanOptimizer(DatabaseInstance &db);
	// static void RegisterExprOptimizer(DatabaseInstance &db);
	// static void RegisterTopKOptimizer(DatabaseInstance &db);
	// static void RegisterJoinOptimizer(DatabaseInstance &db);
};

} // namespace duckdb
