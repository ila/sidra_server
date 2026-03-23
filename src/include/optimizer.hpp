#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

//! Optimizer rule that intercepts DROP TABLE operations to clean up SIDRA metadata.
//! When a SIDRA table is dropped, this rule removes the corresponding entries from
//! sidra_tables, sidra_table_constraints, sidra_view_constraints, and sidra_current_window.
class SIDRADropTableRule : public OptimizerExtension {
public:
	SIDRADropTableRule() {
		optimize_function = Optimize;
	}

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
