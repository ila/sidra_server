#include "compiler_utils.hpp"

#include <fstream>
#include <regex>
#include <sstream>

namespace duckdb {

void WriteFile(const string &filename, bool append, const string &compiled_query) {
	std::ofstream file;
	if (append) {
		file.open(filename, std::ios_base::app);
	} else {
		file.open(filename);
	}
	file << compiled_query << '\n';
	file.close();
}

string ReadFile(const string &file_path) {
	string content;
	std::ifstream file(file_path);
	if (file.is_open()) {
		string line;
		while (std::getline(file, line)) {
			content += line + "\n";
		}
		file.close();
	}
	return content;
}

string ExtractTableName(const string &sql) {
	std::regex table_name_regex(
	    R"(create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))");
	std::smatch match;
	if (std::regex_search(sql, match, table_name_regex)) {
		return match[1].str();
	}
	return "";
}

string ExtractViewName(const string &sql) {
	std::regex view_name_regex(
	    R"(create\s+(?:materialized\s+)?view\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))");
	std::smatch match;
	if (std::regex_search(sql, match, view_name_regex)) {
		return match[1].str();
	}
	return "";
}

string ExtractViewQuery(string &query) {
	std::regex rgx_create_view(
	    R"(create\s+(table|materialized view)\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)\s+as\s+(.*))");
	std::smatch match;
	if (std::regex_search(query, match, rgx_create_view)) {
		return match[3].str();
	}
	return "";
}

string EscapeSingleQuotes(const string &input) {
	std::stringstream escaped_stream;
	for (char c : input) {
		if (c == '\'') {
			escaped_stream << "''";
		} else {
			escaped_stream << c;
		}
	}
	return escaped_stream.str();
}

void ReplaceMaterializedView(string &query) {
	query = std::regex_replace(query, std::regex("\\bmaterialized\\s+view\\b"), "table if not exists");
	query = std::regex_replace(query, std::regex("\\s*;$"), "");
}

void RemoveRedundantWhitespaces(string &query) {
	query = std::regex_replace(query, std::regex("\\s+"), " ");
}

// Functions needed by LPTS (logical_plan_to_sql.cpp)
string VecToSeparatedList(vector<string> input_list, const string &separator) {
	std::ostringstream ret_str;
	for (size_t i = 0; i < input_list.size(); ++i) {
		ret_str << input_list[i];
		if (i != input_list.size() - 1) {
			ret_str << separator;
		}
	}
	return ret_str.str();
}

string SQLToLowercase(const string &sql) {
	std::stringstream lowercase_stream;
	bool in_string = false;
	for (char c : sql) {
		if (c == '\'') {
			in_string = !in_string;
		}
		if (!in_string) {
			lowercase_stream << static_cast<char>(tolower(c));
		} else {
			lowercase_stream << c;
		}
	}
	return lowercase_stream.str();
}

} // namespace duckdb
