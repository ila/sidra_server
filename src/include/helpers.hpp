#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class TableScope { null = 0, centralized = 1, decentralized = 2, replicated = 3 };

struct SIDRAConstraints {
	bool sensitive = false;
	bool dimension = false;
	bool fact = false;
};

struct SIDRAViewConstraint {
	uint8_t window = 0;
	uint8_t ttl = 0;
	uint8_t refresh = 0;
	uint8_t min_agg = 0;
};

struct SIDRASelectOption {
	float response_ratio = -1;
	float minimum_response = -1;
};

} // namespace duckdb
