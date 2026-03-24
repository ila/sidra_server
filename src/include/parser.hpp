#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "helpers.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Parse data — carries all extracted info from parse → plan
//===--------------------------------------------------------------------===//

struct SIDRAParseData : ParserExtensionParseData {
	string stripped_sql;
	TableScope scope;
	unordered_map<string, SIDRAConstraints> table_constraints;
	SIDRAViewConstraint view_constraint;
	bool is_table = false;
	bool is_view = false;
	string table_name;
	string view_name;
	string view_query;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		auto copy = make_uniq<SIDRAParseData>();
		copy->stripped_sql = stripped_sql;
		copy->scope = scope;
		copy->table_constraints = table_constraints;
		copy->view_constraint = view_constraint;
		copy->is_table = is_table;
		copy->is_view = is_view;
		copy->table_name = table_name;
		copy->view_name = view_name;
		copy->view_query = view_query;
		return std::move(copy);
	}

	string ToString() const override {
		return stripped_sql;
	}
};

//===--------------------------------------------------------------------===//
// Parser extension
//===--------------------------------------------------------------------===//

class SIDRAParserExtension : public ParserExtension {
public:
	SIDRAParserExtension() {
		parse_function = SIDRAParseFunction;
		plan_function = SIDRAPlanFunction;
	}

	static ParserExtensionParseResult SIDRAParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult SIDRAPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                   unique_ptr<ParserExtensionParseData> parse_data);

	string Name();
};

//===--------------------------------------------------------------------===//
// DDL executor — bind-phase execution (PAC pattern)
//===--------------------------------------------------------------------===//

struct SIDRADDLBindData : public TableFunctionData {
	bool executed = false;
};

//! Bind function that executes compiled SIDRA DDL via a separate connection
unique_ptr<FunctionData> SIDRADDLBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names);

//! Execute function that returns empty result (DDL already done in bind)
void SIDRADDLExecuteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

//===--------------------------------------------------------------------===//
// Query execution helpers (used by flush and other pragmas)
//===--------------------------------------------------------------------===//

void ExecuteAndWriteQueries(Connection &con, const string &queries, const string &file_path, bool append);
void ExecuteCommitAndWriteQueries(Connection &con, const string &queries, const string &file_path, bool append,
                                  bool write);
void ExecuteCommitLogAndWriteQueries(Connection &con, const string &queries, const string &file_path,
                                     const string &view_name, bool append, int run, bool write);
string HashQuery(const string &query);

} // namespace duckdb
