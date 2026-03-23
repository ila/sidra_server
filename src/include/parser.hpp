#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "helpers.hpp"

namespace duckdb {

void ExecuteAndWriteQueries(Connection &con, const string &queries, const string &file_path, bool append);
void ExecuteCommitAndWriteQueries(Connection &con, const string &queries, const string &file_path, bool append,
                                  bool write);
void ExecuteCommitLogAndWriteQueries(Connection &con, const string &queries, const string &file_path,
                                     const string &view_name, bool append, int run, bool write);
string HashQuery(const string &query);

//===--------------------------------------------------------------------===//
// Parser extension
//===--------------------------------------------------------------------===//

struct SIDRAParseData : ParserExtensionParseData {
	string query;
	TableScope scope;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq_base<ParserExtensionParseData, SIDRAParseData>(query, scope);
	}

	string ToString() const override {
		return query;
	}

	explicit SIDRAParseData(const string &query, const TableScope &scope) : query(query), scope(scope) {
	}
};

class SIDRAParserExtension : public ParserExtension {
public:
	SIDRAParserExtension() {
		parse_function = SIDRAParseFunction;
		plan_function = SIDRAPlanFunction;
	}

	static ParserExtensionParseResult SIDRAParseFunction(ParserExtensionInfo *info, const string &query);

	static ParserExtensionPlanResult SIDRAPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                   duckdb::unique_ptr<ParserExtensionParseData> parse_data);

	string Name();
	static string path;
	static string db;
};

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
		auto result = Value::BOOLEAN(bind_data.result);
		data.offset++;
		output.SetValue(0, 0, result);
		output.SetCardinality(1);
	}
};

} // namespace duckdb
