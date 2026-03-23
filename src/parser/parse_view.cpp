#include "parse_view.hpp"
#include "compiler_utils.hpp"

#include <regex>
#include <string>

namespace duckdb {

SIDRAViewConstraint ParseCreateView(string &query, TableScope scope) {
	SIDRAViewConstraint constraint;

	std::regex window_regex("\\bwindow\\s+(\\d+)\\b");
	std::smatch window_match;
	std::regex ttl_regex("\\bttl\\s+(\\d+)\\b");
	std::smatch ttl_match;
	std::regex refresh_regex("\\brefresh\\s+(\\d+)\\b");
	std::smatch refresh_match;
	std::regex min_agg_regex("\\bminimum aggregation\\s+(\\d+)\\b");
	std::smatch min_agg_match;

	if (std::regex_search(query, window_match, window_regex)) {
		if (scope == TableScope::centralized || scope == TableScope::replicated) {
			throw ParserException("WINDOW is not allowed for centralized or replicated tables!");
		}
		constraint.window = std::stoi(window_match[1].str());
		if (constraint.window <= 0) {
			throw ParserException("Invalid value for WINDOW, must be greater than zero!");
		}
		query = std::regex_replace(query, window_regex, "");
	} else if (scope == TableScope::decentralized) {
		throw ParserException("WINDOW is a required field!");
	}

	if (std::regex_search(query, ttl_match, ttl_regex)) {
		if (scope == TableScope::centralized || scope == TableScope::replicated) {
			throw ParserException("TTL is not allowed for centralized or replicated tables!");
		}
		constraint.ttl = std::stoi(ttl_match[1].str());
		if (constraint.ttl <= 0) {
			throw ParserException("Invalid value for TTL, must be greater than zero!");
		}
		if (constraint.ttl < constraint.window) {
			throw ParserException("TTL must be greater than or equal to WINDOW!");
		}
		query = std::regex_replace(query, ttl_regex, "");
	} else if (scope == TableScope::decentralized) {
		throw ParserException("TTL is a required field!");
	}

	if (std::regex_search(query, refresh_match, refresh_regex)) {
		constraint.refresh = std::stoi(refresh_match[1].str());
		if (constraint.refresh <= 0) {
			throw ParserException("Invalid value for REFRESH, must be greater than zero!");
		}
		query = std::regex_replace(query, refresh_regex, "");
	} else {
		throw ParserException("REFRESH is a required field!");
	}

	if (std::regex_search(query, min_agg_match, min_agg_regex)) {
		if (scope == TableScope::decentralized) {
			throw ParserException("MINIMUM AGGREGATION is not allowed for decentralized tables!");
		}
		constraint.min_agg = std::stoi(min_agg_match[1].str());
		if (constraint.min_agg <= 0) {
			throw ParserException("Invalid value for MINIMUM AGGREGATION, must be greater than zero!");
		}
		query = std::regex_replace(query, min_agg_regex, "");
	}

	return constraint;
}

string ParseViewTables(string &query) {
	std::regex from_clause("FROM\\s+([^\\s,]+)(?:\\s+(?:JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN)\\s+([^\\s,]+))?");
	std::vector<std::string> tables;

	std::sregex_iterator it(query.begin(), query.end(), from_clause);
	std::sregex_iterator end;

	for (; it != end; ++it) {
		const std::smatch &match = *it;
		tables.push_back(match[1]);
		if (match[2].str().length() > 0) {
			tables.push_back(match[2]);
		}
	}

	string in;
	for (auto t = tables.begin(); t != tables.end(); ++t) {
		if (t != tables.begin()) {
			in += ", ";
		}
		in += *t;
	}

	return in;
}

} // namespace duckdb
