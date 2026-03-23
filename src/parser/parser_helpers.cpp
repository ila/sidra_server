#include "parser_helpers.hpp"
#include "server_debug.hpp"

#include <regex>
#include <sstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Safe number parsing
//===--------------------------------------------------------------------===//

int SafeStoi(const string &str, const string &field_name) {
	try {
		return std::stoi(str);
	} catch (const std::exception &e) {
		throw ParserException("Invalid integer value for " + field_name + ": '" + str + "'");
	}
}

double SafeStod(const string &str, const string &field_name) {
	try {
		return std::stod(str);
	} catch (const std::exception &e) {
		throw ParserException("Invalid numeric value for " + field_name + ": '" + str + "'");
	}
}

//===--------------------------------------------------------------------===//
// Query preprocessing
//===--------------------------------------------------------------------===//

string SQLToLowerPreservingStrings(const string &sql) {
	std::stringstream result;
	bool in_string = false;
	for (char c : sql) {
		if (c == '\'') {
			in_string = !in_string;
		}
		if (!in_string) {
			result << static_cast<char>(tolower(c));
		} else {
			result << c;
		}
	}
	return result.str();
}

string CleanQuery(const string &query) {
	string q = query;

	// Strip line comments (-- to end of line)
	q = std::regex_replace(q, std::regex(R"(--[^\n]*)"), "");

	// Strip block comments (/* ... */)
	q = std::regex_replace(q, std::regex(R"(/\*[\s\S]*?\*/)"), "");

	// Replace newlines, tabs, carriage returns with spaces
	q = std::regex_replace(q, std::regex(R"([\n\r\t])"), " ");

	// Collapse multiple whitespace into single space
	q = std::regex_replace(q, std::regex(R"(\s+)"), " ");

	// Trim
	StringUtil::Trim(q);

	// Lowercase preserving string literals
	q = SQLToLowerPreservingStrings(q);

	// Ensure trailing semicolon
	if (q.empty() || q.back() != ';') {
		q += ";";
	}

	return q;
}

//===--------------------------------------------------------------------===//
// Scope extraction
//===--------------------------------------------------------------------===//

TableScope ExtractScope(string &query) {
	// Match: CREATE [CENTRALIZED|DECENTRALIZED|REPLICATED] [TABLE|MATERIALIZED VIEW] ...
	static const std::regex SCOPE_REGEX(
	    R"(\b(create)\s+(centralized|decentralized|replicated)\s+(table|materialized\s+view)\s+(.*))",
	    std::regex_constants::icase);
	std::smatch match;

	// Non-materialized views are not allowed
	if (std::regex_search(query, std::regex(R"(\bview\b)", std::regex_constants::icase)) &&
	    !std::regex_search(query, std::regex(R"(\bmaterialized\b)", std::regex_constants::icase))) {
		throw ParserException("Views should be materialized!");
	}

	if (!std::regex_search(query, match, SCOPE_REGEX)) {
		return TableScope::null;
	}

	if (match.size() != 5) {
		return TableScope::null;
	}

	string scope_str = StringUtil::Lower(match[2].str());
	string object_type = StringUtil::Lower(match[3].str());
	TableScope scope;

	if (scope_str == "centralized") {
		scope = TableScope::centralized;
	} else if (scope_str == "decentralized") {
		scope = TableScope::decentralized;
	} else {
		scope = TableScope::replicated;
	}

	// Replicated must be a materialized view
	if (scope == TableScope::replicated && object_type.find("materialized") == string::npos) {
		throw ParserException("Replicated tables must be materialized views!");
	}

	// Rebuild query without the scope keyword
	query = match[1].str() + " " + match[3].str() + " " + match[4].str();

	SERVER_DEBUG_PRINT("Extracted scope: " + scope_str);
	return scope;
}

//===--------------------------------------------------------------------===//
// Table constraint extraction
//===--------------------------------------------------------------------===//

unordered_map<string, SIDRAConstraints> ExtractTableConstraints(string &query) {
	unordered_map<string, SIDRAConstraints> result;

	// Extract everything between the outermost parentheses
	static const std::regex COLUMNS_REGEX(R"(\((.+)\))", std::regex_constants::icase);
	std::smatch columns_match;
	if (!std::regex_search(query, columns_match, COLUMNS_REGEX)) {
		return result;
	}

	string columns_definition = columns_match[1].str();

	// Split columns by comma
	static const std::regex SPLIT_REGEX(R"(,\s*)");
	std::sregex_token_iterator it(columns_definition.begin(), columns_definition.end(), SPLIT_REGEX, -1);
	std::sregex_token_iterator end;

	for (; it != end; ++it) {
		string col_def = it->str();
		StringUtil::Trim(col_def);

		// Extract column name (first word)
		static const std::regex COL_NAME_REGEX(R"(\b(\w+)\s)");
		std::smatch col_match;
		if (!std::regex_search(col_def, col_match, COL_NAME_REGEX)) {
			continue;
		}

		string column_name = col_match.str(1);
		SIDRAConstraints constraints;

		// Check for SIDRA constraints using proper word boundaries
		static const std::regex FACT_REGEX(R"(\bfact\b)", std::regex_constants::icase);
		static const std::regex SENSITIVE_REGEX(R"(\bsensitive\b)", std::regex_constants::icase);
		static const std::regex DIMENSION_REGEX(R"(\bdimension\b)", std::regex_constants::icase);

		if (std::regex_search(col_def, FACT_REGEX)) {
			constraints.fact = true;
		}
		if (std::regex_search(col_def, SENSITIVE_REGEX)) {
			constraints.sensitive = true;
		}
		if (std::regex_search(col_def, DIMENSION_REGEX)) {
			constraints.dimension = true;
		}

		if (constraints.fact || constraints.sensitive || constraints.dimension) {
			result.insert(make_pair(column_name, constraints));
		}
	}

	// Strip constraint keywords from query
	static const std::regex STRIP_CONSTRAINTS_REGEX(R"(\b(sensitive|fact|dimension)\b)", std::regex_constants::icase);
	query = std::regex_replace(query, STRIP_CONSTRAINTS_REGEX, "");

	SERVER_DEBUG_PRINT("Extracted " + to_string(result.size()) + " column constraints");
	return result;
}

//===--------------------------------------------------------------------===//
// View constraint extraction
//===--------------------------------------------------------------------===//

SIDRAViewConstraint ExtractViewConstraints(string &query, TableScope scope) {
	SIDRAViewConstraint constraint;

	// WINDOW
	static const std::regex WINDOW_REGEX(R"(\bwindow\s+(\d+)\b)", std::regex_constants::icase);
	std::smatch window_match;
	if (std::regex_search(query, window_match, WINDOW_REGEX)) {
		if (scope == TableScope::centralized || scope == TableScope::replicated) {
			throw ParserException("WINDOW is not allowed for centralized or replicated tables!");
		}
		constraint.window = SafeStoi(window_match[1].str(), "WINDOW");
		if (constraint.window <= 0) {
			throw ParserException("Invalid value for WINDOW, must be greater than zero!");
		}
		query = std::regex_replace(query, WINDOW_REGEX, "");
	} else if (scope == TableScope::decentralized) {
		throw ParserException("WINDOW is a required field!");
	}

	// TTL
	static const std::regex TTL_REGEX(R"(\bttl\s+(\d+)\b)", std::regex_constants::icase);
	std::smatch ttl_match;
	if (std::regex_search(query, ttl_match, TTL_REGEX)) {
		if (scope == TableScope::centralized || scope == TableScope::replicated) {
			throw ParserException("TTL is not allowed for centralized or replicated tables!");
		}
		constraint.ttl = SafeStoi(ttl_match[1].str(), "TTL");
		if (constraint.ttl <= 0) {
			throw ParserException("Invalid value for TTL, must be greater than zero!");
		}
		if (constraint.ttl < constraint.window) {
			throw ParserException("TTL must be greater than or equal to WINDOW!");
		}
		query = std::regex_replace(query, TTL_REGEX, "");
	} else if (scope == TableScope::decentralized) {
		throw ParserException("TTL is a required field!");
	}

	// REFRESH
	static const std::regex REFRESH_REGEX(R"(\brefresh\s+(\d+)\b)", std::regex_constants::icase);
	std::smatch refresh_match;
	if (std::regex_search(query, refresh_match, REFRESH_REGEX)) {
		constraint.refresh = SafeStoi(refresh_match[1].str(), "REFRESH");
		if (constraint.refresh <= 0) {
			throw ParserException("Invalid value for REFRESH, must be greater than zero!");
		}
		query = std::regex_replace(query, REFRESH_REGEX, "");
	} else {
		throw ParserException("REFRESH is a required field!");
	}

	// MINIMUM AGGREGATION
	static const std::regex MIN_AGG_REGEX(R"(\bminimum\s+aggregation\s+(\d+)\b)", std::regex_constants::icase);
	std::smatch min_agg_match;
	if (std::regex_search(query, min_agg_match, MIN_AGG_REGEX)) {
		if (scope == TableScope::decentralized) {
			throw ParserException("MINIMUM AGGREGATION is not allowed for decentralized tables!");
		}
		constraint.min_agg = SafeStoi(min_agg_match[1].str(), "MINIMUM AGGREGATION");
		if (constraint.min_agg <= 0) {
			throw ParserException("Invalid value for MINIMUM AGGREGATION, must be greater than zero!");
		}
		query = std::regex_replace(query, MIN_AGG_REGEX, "");
	}

	SERVER_DEBUG_PRINT("Extracted view constraints: window=" + to_string(constraint.window) +
	                   " ttl=" + to_string(constraint.ttl) + " refresh=" + to_string(constraint.refresh) +
	                   " min_agg=" + to_string(constraint.min_agg));
	return constraint;
}

//===--------------------------------------------------------------------===//
// Select option extraction
//===--------------------------------------------------------------------===//

SIDRASelectOption ExtractSelectOptions(string &query) {
	SIDRASelectOption options;

	// Match OPTION(...) at the end of a SELECT
	static const std::regex OPTION_REGEX(R"(\bOPTION\s*\(([^)]+)\))", std::regex_constants::icase);
	std::smatch option_match;

	if (!std::regex_search(query, option_match, OPTION_REGEX)) {
		return options;
	}

	string option_string = option_match[1].str();

	// Remove OPTION clause from query
	query =
	    std::regex_replace(query, std::regex(R"(\s*\bOPTION\s*\([^)]+\)\s*;?\s*$)", std::regex_constants::icase), "");

	// RESPONSE RATIO
	static const std::regex RESPONSE_RATIO_REGEX(R"(\bresponse\s+ratio\s+(\d+\.?\d*))", std::regex_constants::icase);
	std::smatch rr_match;
	if (std::regex_search(option_string, rr_match, RESPONSE_RATIO_REGEX)) {
		options.response_ratio = SafeStod(rr_match[1].str(), "RESPONSE RATIO");
		if (options.response_ratio < 0 || options.response_ratio > 1) {
			throw ParserException("Response ratio should be between 0 and 1.");
		}
	}

	// MINIMUM RESPONSE
	static const std::regex MIN_RESPONSE_REGEX(R"(\bminimum\s+response\s+(\d+\.?\d*))", std::regex_constants::icase);
	std::smatch mr_match;
	if (std::regex_search(option_string, mr_match, MIN_RESPONSE_REGEX)) {
		options.minimum_response = SafeStod(mr_match[1].str(), "MINIMUM RESPONSE");
		if (options.minimum_response < 0 || options.minimum_response > 1) {
			throw ParserException("Minimum response should be between 0 and 1.");
		}
	}

	SERVER_DEBUG_PRINT("Extracted select options: response_ratio=" + to_string(options.response_ratio) +
	                   " minimum_response=" + to_string(options.minimum_response));
	return options;
}

} // namespace duckdb
