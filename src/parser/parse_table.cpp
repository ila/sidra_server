#include "parse_table.hpp"

#include <regex>
#include <string>
#include <vector>

namespace duckdb {

TableScope ParseScope(std::string &query) {
	std::regex scope_regex(
	    "\\b(create)\\s+(centralized|decentralized|replicated)\\s+(table|materialized\\s+view)\\s+(.*)");
	std::smatch scope_match;
	TableScope scope = TableScope::null;

	if (std::regex_search(query, std::regex("\\bview\\b")) &&
	    !std::regex_search(query, std::regex("\\bmaterialized\\b"))) {
		throw ParserException("Views should be materialized!");
	}

	std::regex_search(query, scope_match, scope_regex);
	if (scope_match.size() == 5) {
		if ((scope_match[2].str() == "centralized" &&
		     (scope_match[3].str() == "decentralized" || scope_match[3].str() == "replicated")) ||
		    (scope_match[2].str() == "decentralized" &&
		     (scope_match[3].str() == "centralized" || scope_match[3].str() == "replicated")) ||
		    (scope_match[2].str() == "replicated" &&
		     (scope_match[3].str() == "decentralized" || scope_match[3].str() == "centralized"))) {
			throw ParserException("Cannot specify multiple table scopes");
		}
		if (scope_match[2].str() == "centralized") {
			scope = TableScope::centralized;
		} else if (scope_match[2].str() == "decentralized") {
			scope = TableScope::decentralized;
		} else {
			scope = TableScope::replicated;
		}
		query = scope_match[1].str() + " " + scope_match[3].str() + " " + scope_match[4].str();
	}

	if (scope == TableScope::replicated && !std::regex_search(query, std::regex("\\bmaterialized\\s+view\\b"))) {
		throw ParserException("Replicated tables must be materialized views!");
	}

	return scope;
}

unordered_map<string, SIDRAConstraints> ParseCreateTable(std::string &query) {
	unordered_map<string, SIDRAConstraints> constraints_table;

	std::regex columns_regex("\\((.*)\\)");
	std::smatch columns_match;
	if (std::regex_search(query, columns_match, columns_regex)) {
		std::string columns_definition = columns_match[1].str();

		std::regex column_name_regex(R"(\b(\w+)\s)");
		std::regex split_regex(",\\s*");
		std::sregex_token_iterator split_iter(columns_definition.begin(), columns_definition.end(), split_regex, -1);
		std::sregex_token_iterator split_end;

		for (; split_iter != split_end; split_iter++) {
			std::string split = split_iter->str();
			StringUtil::Trim(split);

			SIDRAConstraints constraints_column;
			string column_name;
			std::smatch column_name_matches;
			if (std::regex_search(split, column_name_matches, column_name_regex)) {
				column_name = column_name_matches.str(1);
			}

			if (std::regex_search(split, std::regex("\\bfact\\b"))) {
				constraints_column.fact = true;
			}

			if (std::regex_search(split, std::regex("\\bsensitive\\b"))) {
				constraints_column.sensitive = true;
			}

			if (std::regex_search(split, std::regex("\\bdimension\\b"))) {
				constraints_column.dimension = true;
			}

			StringUtil::Trim(split);
			if (constraints_column.fact || constraints_column.sensitive || constraints_column.dimension) {
				constraints_table.insert(make_pair(column_name, constraints_column));
			}
		}

		query = std::regex_replace(query, std::regex("\\b(sensitive|fact|dimension)\\b"), "");
	}

	return constraints_table;
}

} // namespace duckdb
