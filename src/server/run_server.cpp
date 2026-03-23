#include "run_server.hpp"
#include "common.hpp"
#include "compiler_utils.hpp"
#include "server_debug.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/planner.hpp"

#include <atomic>
#include <csignal>
#include <mutex>
#include <netinet/in.h>
#include <thread>

namespace duckdb {

// Global state for server shutdown
static volatile sig_atomic_t server_running = true;

// Self-pipe for waking up select() after receiving a signal
static int self_pipe[2];

void SignalHandler(int signal) {
	if (signal == SIGINT || signal == SIGTERM) {
		Printer::Print("\nReceived shutdown signal, stopping server...");
		server_running = false;

		// Write to self-pipe to wake up select()
		char buf = 1;
		auto bytes_written = write(self_pipe[1], &buf, 1);
		(void)bytes_written;
	}
}

static void SetupSignalHandling() {
	std::signal(SIGINT, SignalHandler);
	std::signal(SIGTERM, SignalHandler);
}

static void CleanupSockets(int sockfd, vector<int32_t> &client_socket) {
	close(sockfd);
	for (auto fd : client_socket) {
		if (fd > 0) {
			close(fd);
		}
	}
	close(self_pipe[0]);
	close(self_pipe[1]);
}

void SerializeQueryPlan(string &query, string &path, string &dbname) {
	DuckDB db(path + dbname);
	Connection con(db);

	BufferedFileWriter target(db.GetFileSystem(), path + "sidra_serialized_plan.binary");

	con.BeginTransaction();
	Parser p;
	p.ParseQuery(query);

	Planner planner(*con.context);
	planner.CreatePlan(std::move(p.statements[0]));
	auto plan = std::move(planner.plan);

	Optimizer optimizer(*planner.binder, *con.context);
	plan = optimizer.Optimize(std::move(plan));

	BinarySerializer serializer(target);
	serializer.Begin();
	plan->Serialize(serializer);
	serializer.End();

	con.Rollback();
	target.Sync();
}

void DeserializeQueryPlan(string &path, string &dbname) {
	string file_path = path + "sidra_serialized_plan.binary";

	DuckDB db(path + dbname);
	Connection con(db);

	BufferedFileReader file_source(db.GetFileSystem(), file_path.c_str());

	con.Query("PRAGMA enable_profiling=json");
	con.Query("PRAGMA profile_output='profile_output.json'");

	BinaryDeserializer deserializer(file_source);
	deserializer.Set<ClientContext &>(*con.context);
	deserializer.Begin();
	auto plan = LogicalOperator::Deserialize(deserializer);
	deserializer.End();

	plan->Print();
	plan->ResolveOperatorTypes();

	// TODO: execute the deserialized plan
	// auto statement = make_uniq<LogicalPlanStatement>(std::move(plan));
	// auto result = con.Query(std::move(statement));
	// result->Print();
}

void InsertClient(Connection &con, unordered_map<string, string> &config, uint64_t id, const string &timestamp) {
	string table_name = "sidra_clients";
	string query = "insert or ignore into " + table_name + " values (" + std::to_string(id) + ", '" +
	               EscapeSingleQuotes(timestamp) + "', '" + EscapeSingleQuotes(timestamp) + "');";
	auto r = con.Query(query);
	if (r->HasError()) {
		throw ParserException("Error while inserting client information: " + r->GetError());
	}
	Printer::Print("Added client: " + std::to_string(id));
}

static void InsertNewClient(int32_t connfd, Connection &con, unordered_map<string, string> &config) {
	uint64_t id;
	uint64_t timestamp_size;

	if (read(connfd, &id, sizeof(uint64_t)) <= 0) {
		throw IOException("Failed to read client id");
	}
	if (read(connfd, &timestamp_size, sizeof(uint64_t)) <= 0) {
		throw IOException("Failed to read timestamp size");
	}

	vector<char> timestamp(timestamp_size);
	if (read(connfd, timestamp.data(), timestamp_size) <= 0) {
		throw IOException("Failed to read timestamp");
	}
	string timestamp_string(timestamp.begin(), timestamp.end());

	InsertClient(con, config, id, timestamp_string);

	string file_name = "decentralized_queries.sql";
	auto queries = ReadFile(file_name);

	if (queries.empty()) {
		throw ParserException("Missing query file: " + file_name);
	}

	size_t size = queries.size();
	send(connfd, &size, sizeof(size_t), 0);
	send(connfd, queries.c_str(), size, 0);
	Printer::Print("Sent queries to client!");
}

static void InsertNewResult(int32_t connfd, Connection &metadata_con, Connection &client_con,
                            unordered_map<string, string> &config) {
	uint64_t id;
	if (read(connfd, &id, sizeof(uint64_t)) <= 0) {
		throw IOException("Failed to read client id");
	}

	auto client_info = metadata_con.Query("SELECT * FROM sidra_clients WHERE id = " + to_string(id));
	if (client_info->RowCount() == 0) {
		int32_t error = ERROR_NON_EXISTING_CLIENT;
		send(connfd, &error, sizeof(int32_t), 0);
		return;
	}

	int32_t ok_message = OK;
	send(connfd, &ok_message, sizeof(int32_t), 0);

	size_t view_name_size;
	if (read(connfd, &view_name_size, sizeof(size_t)) <= 0) {
		throw IOException("Failed to read view name size");
	}

	vector<char> view_name(view_name_size);
	if (read(connfd, view_name.data(), view_name_size) <= 0) {
		throw IOException("Failed to read view name");
	}

	uint64_t timestamp_size;
	if (read(connfd, &timestamp_size, sizeof(uint64_t)) <= 0) {
		throw IOException("Failed to read timestamp size");
	}

	vector<char> timestamp(timestamp_size);
	if (read(connfd, timestamp.data(), timestamp_size) <= 0) {
		throw IOException("Failed to read timestamp");
	}

	idx_t n_chunks;
	if (read(connfd, &n_chunks, sizeof(idx_t)) <= 0) {
		throw IOException("Failed to read chunk count");
	}

	string view_name_str(view_name.begin(), view_name.end());
	string timestamp_str(timestamp.begin(), timestamp.end());

	string centralized_view =
	    (config["schema_name"] != "main" ? config["schema_name"] + "." : "") + "sidra_staging_view_" + view_name_str;

	auto view_info = client_con.TableInfo(centralized_view);
	if (!view_info) {
		throw ParserException("Centralized view not found: " + centralized_view);
	}

	auto window_query =
	    "select sidra_window from sidra_current_window where view_name = 'sidra_staging_view_" + view_name_str + "';";
	auto r = metadata_con.Query(window_query);
	if (r->HasError() || r->RowCount() == 0) {
		throw ParserException("Error while querying window metadata: " + r->GetError());
	}
	auto window = std::stoi(r->GetValue(0, 0).ToString());

	client_con.BeginTransaction();
	Appender appender(client_con, centralized_view);

	auto ts_gen = Timestamp::FromString(timestamp_str, false);
	auto ts_arr = Timestamp::GetCurrentTimestamp();

	for (idx_t i = 0; i < n_chunks; i++) {
		ssize_t chunk_len;
		if (read(connfd, &chunk_len, sizeof(ssize_t)) <= 0) {
			throw IOException("Failed to read chunk length");
		}

		vector<char> buffer(chunk_len);
		if (read(connfd, buffer.data(), chunk_len) <= 0) {
			throw IOException("Failed to read chunk data");
		}

		MemoryStream stream((data_ptr_t)buffer.data(), chunk_len);
		BinaryDeserializer deserializer(stream);
		DataChunk chunk;
		chunk.Deserialize(deserializer);

		for (idx_t row = 0; row < chunk.size(); row++) {
			appender.BeginRow();
			for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
				appender.Append(chunk.GetValue(col, row));
			}
			appender.Append(ts_gen);
			appender.Append(ts_arr);
			appender.Append<int32_t>(window);
			appender.Append<uint64_t>(id);
			appender.Append<int8_t>(1); // TODO: change the action
			appender.EndRow();
		}
	}
	appender.Close();
	client_con.Commit();
	Printer::Print("New result inserted for the view: " + view_name_str + "...");
}

static void InsertNewStatistics(int32_t connfd) {
	// TODO: implement statistics collection
	SERVER_DEBUG_PRINT("InsertNewStatistics: not yet implemented");
}

static void UpdateTimestampClient(int32_t connfd, Connection &con) {
	uint64_t id;
	uint64_t timestamp_size;

	if (read(connfd, &id, sizeof(uint64_t)) <= 0) {
		throw IOException("Failed to read client id");
	}
	if (read(connfd, &timestamp_size, sizeof(uint64_t)) <= 0) {
		throw IOException("Failed to read timestamp size");
	}

	vector<char> timestamp(timestamp_size);
	if (read(connfd, timestamp.data(), timestamp_size) <= 0) {
		throw IOException("Failed to read timestamp");
	}
	string timestamp_string(timestamp.begin(), timestamp.end());

	con.BeginTransaction();
	con.Query("update sidra_clients set last_update = '" + EscapeSingleQuotes(timestamp_string) +
	          "' where id = " + std::to_string(id) + ";");
	con.Commit();
}

// Global mutex for thread-safe operations on shared resources
static std::mutex client_socket_mutex;

static void CloseConnection(int connfd, vector<int32_t> &client_socket, int index) {
	std::lock_guard<std::mutex> lock(client_socket_mutex);
	Printer::Print("Host disconnected!");
	close(connfd);
	client_socket[index] = 0;
}

static void FlushRemotely(int32_t connfd) {
	size_t view_name_size;
	if (read(connfd, &view_name_size, sizeof(size_t)) <= 0) {
		throw IOException("Failed to read view name size");
	}
	vector<char> view_name(view_name_size);
	if (read(connfd, view_name.data(), view_name_size) <= 0) {
		throw IOException("Failed to read view name");
	}
	string view_name_str(view_name.begin(), view_name.end());

	size_t db_name_size;
	if (read(connfd, &db_name_size, sizeof(size_t)) <= 0) {
		throw IOException("Failed to read db name size");
	}
	vector<char> db_name(db_name_size);
	if (read(connfd, db_name.data(), db_name_size) <= 0) {
		throw IOException("Failed to read db name");
	}
	string db_name_str(db_name.begin(), db_name.end());

	DuckDB db(nullptr);
	Connection con(db);
	auto r = con.Query("pragma flush('" + EscapeSingleQuotes(view_name_str) + "', '" + EscapeSingleQuotes(db_name_str) +
	                   "');");
	if (r->HasError()) {
		throw ParserException("Error while flushing view: " + r->GetError());
	}
	Printer::Print("Flushed view: " + view_name_str + "...");
}

static void UpdateWindowRemotely(int32_t connfd, Connection &con) {
	size_t view_name_size;
	if (read(connfd, &view_name_size, sizeof(size_t)) <= 0) {
		throw IOException("Failed to read view name size");
	}
	vector<char> view_name(view_name_size);
	if (read(connfd, view_name.data(), view_name_size) <= 0) {
		throw IOException("Failed to read view name");
	}
	string view_name_str(view_name.begin(), view_name.end());

	auto r = con.Query("update sidra_current_window set sidra_window = sidra_window + 1, last_update = now() "
	                   "where view_name = '" +
	                   EscapeSingleQuotes(view_name_str) + "';");
	if (r->HasError()) {
		throw ParserException("Error while updating window metadata: " + r->GetError());
	}
	Printer::Print("Updated window for view: " + view_name_str + "...");
}

static void ClientThreadHandler(int connfd, unordered_map<string, string> &config, vector<int32_t> &client_socket,
                                int index) {
	while (true) {
		client_messages message;
		ssize_t read_bytes = read(connfd, &message, sizeof(int32_t));

		if (read_bytes <= 0) {
			CloseConnection(connfd, client_socket, index);
			return;
		}

		SERVER_DEBUG_PRINT("Reading message: " + ToString(message) + "...");

		string metadata_db_name = "sidra_parser.db";
		string client_db_name = "sidra_client.db";
		DuckDB metadata_db(metadata_db_name);
		DuckDB client_db(client_db_name);
		Connection metadata_con(metadata_db);
		Connection client_con(client_db);

		switch (message) {
		case CLOSE_CONNECTION:
			CloseConnection(connfd, client_socket, index);
			return;

		case NEW_CLIENT:
			InsertNewClient(connfd, metadata_con, config);
			break;

		case NEW_RESULT:
			InsertNewResult(connfd, metadata_con, client_con, config);
			break;

		case NEW_STATISTICS:
			InsertNewStatistics(connfd);
			break;

		case UPDATE_TIMESTAMP_CLIENT:
			UpdateTimestampClient(connfd, metadata_con);
			break;

		case FLUSH:
			FlushRemotely(connfd);
			break;

		case UPDATE_WINDOW:
			UpdateWindowRemotely(connfd, metadata_con);
			break;

		default:
			Printer::Print("Unknown message received: " + ToString(message));
			CloseConnection(connfd, client_socket, index);
			break;
		}
	}
}

void RunServer(ClientContext &context, const FunctionParameters &parameters) {
	server_running = true;
	SetupSignalHandling();

	// Ignore SIGPIPE to prevent crashing when writing to closed sockets
	std::signal(SIGPIPE, SIG_IGN);

	string config_path;
	Value config_path_val;
	if (context.TryGetCurrentSetting("sidra_config_path", config_path_val)) {
		config_path = config_path_val.ToString();
	}
	string config_file = "server.config";
	auto config = ParseConfig(config_path, config_file);

	int32_t max_clients = std::stoi(config["max_clients"]);
	int32_t server_port = std::stoi(config["server_port"]);

	sockaddr_in servaddr {};
	memset(&servaddr, 0, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = INADDR_ANY;
	servaddr.sin_port = htons(server_port);

	int32_t sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sockfd == -1) {
		Printer::Print("Socket creation not successful");
		return;
	}

	int opt = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
		Printer::Print("setsockopt failed");
		close(sockfd);
		return;
	}

	struct linger sl;
	sl.l_onoff = 1;
	sl.l_linger = 0;
	if (setsockopt(sockfd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl))) {
		Printer::Print("setsockopt(SO_LINGER) failed");
		close(sockfd);
		return;
	}

	if (bind(sockfd, reinterpret_cast<struct sockaddr *>(&servaddr), sizeof(servaddr)) != 0) {
		Printer::Print("Bind not successful!");
		close(sockfd);
		return;
	}

	if (listen(sockfd, 10) != 0) {
		Printer::Print("Listen not successful!");
		close(sockfd);
		return;
	}

	Printer::Print("Ready to accept connections on port " + std::to_string(server_port) + "!");

	if (pipe(self_pipe) == -1) {
		Printer::Print("Failed to create self-pipe!");
		close(sockfd);
		return;
	}

	vector<int32_t> client_socket(max_clients, 0);
	vector<std::thread> client_threads;
	socklen_t addrlen = sizeof(servaddr);

	try {
		while (server_running) {
			fd_set readfds;
			FD_ZERO(&readfds);
			FD_SET(sockfd, &readfds);
			FD_SET(self_pipe[0], &readfds);

			int32_t max_sockfd = std::max(sockfd, self_pipe[0]);

			// Clean up finished threads
			auto it = client_threads.begin();
			while (it != client_threads.end()) {
				if (it->joinable()) {
					it->join();
					it = client_threads.erase(it);
				} else {
					++it;
				}
			}

			struct timeval timeout;
			timeout.tv_sec = 5;
			timeout.tv_usec = 0;

			int activity = select(max_sockfd + 1, &readfds, nullptr, nullptr, &timeout);

			if (activity < 0 && errno != EINTR) {
				Printer::Print("Select error: " + std::to_string(errno));
				break;
			}

			if (!server_running) {
				break;
			}

			if (FD_ISSET(self_pipe[0], &readfds)) {
				char buf;
				auto bytes_read_pipe = read(self_pipe[0], &buf, 1);
				(void)bytes_read_pipe;
				break;
			}

			if (FD_ISSET(sockfd, &readfds)) {
				int connfd = accept(sockfd, reinterpret_cast<struct sockaddr *>(&servaddr), &addrlen);
				if (connfd < 0) {
					Printer::Print("Connection error!");
					continue;
				}

				std::lock_guard<std::mutex> lock(client_socket_mutex);
				for (int i = 0; i < max_clients; i++) {
					if (client_socket[i] == 0) {
						client_socket[i] = connfd;

						client_threads.emplace_back([connfd, &config, &client_socket, i]() {
							ClientThreadHandler(connfd, config, client_socket, i);
						});

						break;
					}
				}
			}
		}
	} catch (const std::exception &e) {
		Printer::Print("Interrupting execution and cleaning up sockets.");
	}

	for (auto &thread : client_threads) {
		if (thread.joinable()) {
			thread.join();
		}
	}

	CleanupSockets(sockfd, client_socket);
	Printer::Print("Server shut down cleanly.");
}

} // namespace duckdb
