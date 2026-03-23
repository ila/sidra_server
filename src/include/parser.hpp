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
// Result table function — returns a boolean indicating success
//===--------------------------------------------------------------------===//

class SIDRAFunction : public TableFunction {
public:
	SIDRAFunction() {
		name = "SIDRA function";
		arguments.push_back(LogicalType::BOOLEAN);
		bind = SIDRABind;
		init_global = SIDRAInit;
		function = SIDRAFunc;
	}

	struct SIDRABindData : TableFunctionData {
		explicit SIDRABindData(bool result) : result(result) {
		}
		bool result;
	};

	struct SIDRAGlobalData : GlobalTableFunctionState {
		SIDRAGlobalData() : offset(0) {
		}
		idx_t offset;
	};

	static unique_ptr<FunctionData> SIDRABind(ClientContext &context, TableFunctionBindInput &input,
	                                          vector<LogicalType> &return_types, vector<string> &names) {
		names.emplace_back("SIDRA OBJECT CREATION");
		return_types.emplace_back(LogicalType::BOOLEAN);
		bool result = IntegerValue::Get(input.inputs[0]) == 1;
		return make_uniq<SIDRABindData>(result);
	}

	static unique_ptr<GlobalTableFunctionState> SIDRAInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<SIDRAGlobalData>();
	}

	static void SIDRAFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bind_data = data_p.bind_data->Cast<SIDRABindData>();
		auto &data = dynamic_cast<SIDRAGlobalData &>(*data_p.global_state);
		if (data.offset >= 1) {
			return;
		}
		output.SetValue(0, 0, Value::BOOLEAN(bind_data.result));
		output.SetCardinality(1);
		data.offset++;
	}
};

//===--------------------------------------------------------------------===//
// Query execution helpers (used by plan function and flush)
//===--------------------------------------------------------------------===//

void ExecuteAndWriteQueries(Connection &con, const string &queries, const string &file_path, bool append);
void ExecuteCommitAndWriteQueries(Connection &con, const string &queries, const string &file_path, bool append,
                                  bool write);
void ExecuteCommitLogAndWriteQueries(Connection &con, const string &queries, const string &file_path,
                                     const string &view_name, bool append, int run, bool write);
string HashQuery(const string &query);

} // namespace duckdb
