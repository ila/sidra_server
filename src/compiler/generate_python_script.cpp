#include "generate_python_script.hpp"

#include "duckdb/common/local_file_system.hpp"

#include <fstream>

namespace duckdb {

void GenerateServerRefreshScript(ClientContext &context, const FunctionParameters &parameters) {
	auto database_name = parameters.values[0].GetValue<string>();
	LocalFileSystem fs;
	if (!fs.FileExists(database_name + ".db") && !fs.FileExists(database_name)) {
		throw ParserException("Database does not exist!");
	}

	if (database_name.size() > 3 && database_name.substr(database_name.size() - 3) == ".db") {
		database_name = database_name.substr(0, database_name.size() - 3);
	}
	string file_name = "refresh_server_" + database_name + ".py";

	std::ofstream file;
	file.open(file_name);
	file << "# this script is machine generated - do not edit!\n\n";
	file << "#!/usr/bin/env python3\n";
	file << "import importlib.util\n";
	file << "import sys\n";
	file << "import time\n";
	file << "import os\n";
	file << "from datetime import datetime, timedelta\n";
	file << "import subprocess\n\n";
	file << "current_dir = os.getcwd()\n";
	file << "if importlib.util.find_spec('duckdb') is None:\n";
	file << "\ttry:\n";
	file << "\t\tos.chdir('..')\n\n";
	file << "\t\tenv = os.environ.copy()\n";
	file << "\t\tenv[\"BUILD_PYTHON\"] = \"1\"\n";
	file << "\t\tenv[\"GEN\"] = \"ninja\"\n\n";
	file << "\t\tsubprocess.run([\"make\"], env=env, check=True)\n";
	file << "\t\tsubprocess.run([\"python\", \"setup.py\", \"install\"], cwd=\"tools/pythonpkg\", check=True)\n\n";
	file << "\texcept subprocess.CalledProcessError as e:\n";
	file << "\t\tprint(f\"Command failed with exit code {e.returncode}: {e.cmd}\")\n";
	file << "\t\tprint(f\"Error message: {e.output}\")\n";
	file << "\texcept FileNotFoundError as e:\n";
	file << "\t\tprint(f\"File not found: {e.filename}\")\n";
	file << "\texcept Exception as e:\n";
	file << "\t\tprint(f\"An unexpected error occurred: {e}\")\n\n";

	file << "import duckdb\n\n";
	file << "os.chdir(current_dir)\n\n";

	file << "views = []\n";
	file << "windows = []\n";
	file << "refreshes = []\n";
	file << "last_refreshes = []\n";
	file << "last_updates = []\n\n";

	file << "con = duckdb.connect(database = \"sidra_parser.db\", read_only = True)\n";
	file
	    << "con.execute(\"select sidra_view_constraints.view_name, sidra_view_constraints.sidra_window, sidra_refresh, "
	       "last_refresh, last_update from sidra_view_constraints left outer join sidra_current_window on "
	       "sidra_current_window.view_name = concat('sidra_staging_view_', sidra_view_constraints.view_name);\")\n";

	file << "for view in con.fetchall():\n";
	file << "\tviews.append(view[0])\n";
	file << "\twindows.append(view[1])\n";
	file << "\trefreshes.append(view[2])\n";
	file << "\tlast_refreshes.append(view[3])\n";
	file << "\tlast_updates.append(view[4])\n";
	file << "con.close()\n\n";

	file << "while True:\n";
	file << "\tcurrent_time = datetime.now()\n";
	file << "\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Waiting to refresh metadata...')\n";
	file << "\tfor i in range(len(views)):\n";
	file << "\t\tif last_updates[i] is not None:\n";
	file << "\t\t\tif current_time - last_updates[i] >= timedelta(hours=windows[i]):\n";
	file << "\t\t\t\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Updating window for view: "
	        "{views[i]}')\n";
	file << "\t\t\t\tcon = duckdb.connect(database = \"sidra_parser.db\")\n";
	file << "\t\t\t\tupdate_window_query = f\"update sidra_current_window set sidra_window = sidra_window + 1, "
	        "last_update = now() where view_name = 'sidra_staging_view_{views[i]}';\"\n";
	file << "\t\t\t\tcon.execute(update_window_query)\n";
	file << "\t\t\t\tcon.close()\n";
	file << "\t\t\t\tlast_updates[i] = current_time\n";
	file << "\t\tif current_time - last_refreshes[i] >= timedelta(hours=windows[i]):\n";
	file << "\t\t\tcon = duckdb.connect(database = \"" + database_name + "\")\n";
	file << "\t\t\tpragma_query = f\"PRAGMA flush({views[i]}, 'duckdb');\"\n";
	file << "\t\t\tcon.execute(pragma_query)\n";
	file << "\t\t\tcon.close()\n";
	file << "\t\t\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Refreshing view: {views[i]}')\n";
	file << "\t\t\tcon = duckdb.connect(database = \"sidra_parser.db\")\n";
	file << "\t\t\tupdate_refresh_query = f\"update sidra_view_constraints set last_refresh = now() where view_name = "
	        "'{views[i]}';\"\n";
	file << "\t\t\tcon.execute(update_refresh_query)\n";
	file << "\t\t\tcon.close()\n";
	file << "\t\t\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Updating metadata for view: "
	        "{views[i]}')\n";
	file << "\t\t\tlast_refreshes[i] = current_time\n";

	file << "\ttime.sleep(600)\n";

	file.close();
}

void GenerateClientRefreshScript(ClientContext &context, const FunctionParameters &parameters) {
	auto database_name = parameters.values[0].GetValue<string>();
	LocalFileSystem fs;
	if (!fs.FileExists(database_name + ".db")) {
		throw ParserException("Database does not exist!");
	}

	if (database_name.size() > 3 && database_name.substr(database_name.size() - 3) == ".db") {
		database_name = database_name.substr(0, database_name.size() - 3);
	}
	string file_name = "refresh_client_" + database_name + ".py";

	std::ofstream file;
	file.open(file_name);
	file << "# this script is machine generated - do not edit!\n\n";
	file << "#!/usr/bin/env python3\n";
	file << "import duckdb\n";
	file << "import time\n";
	file << "import os\n";
	file << "from datetime import datetime, timedelta\n\n";

	file << "database_name = \"" + database_name + "\"\n\n";

	// Connect to the client database and query view metadata
	file << "con = duckdb.connect(database = database_name + '.db')\n";
	file << "con.execute(\"SELECT name FROM sidra_tables WHERE is_view = true\")\n";
	file << "views = [row[0] for row in con.fetchall()]\n";
	file << "con.close()\n\n";

	// Generate refresh loop
	file << "print(f'Client refresh script for {database_name}')\n";
	file << "print(f'Found {len(views)} views to refresh')\n\n";

	file << "while True:\n";
	file << "\tcurrent_time = datetime.now()\n";
	file << "\tprint(f'[{current_time.strftime(\"%Y-%m-%d %H:%M:%S\")}] Running client refresh cycle...')\n";
	file << "\tfor view in views:\n";
	file << "\t\ttry:\n";
	file << "\t\t\tcon = duckdb.connect(database = database_name + '.db')\n";
	file << "\t\t\tcon.execute(f\"PRAGMA refresh('{view}')\")\n";
	file << "\t\t\tcon.close()\n";
	file << "\t\t\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Refreshed view: {view}')\n";
	file << "\t\texcept Exception as e:\n";
	file << "\t\t\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Error refreshing {view}: {e}')\n";
	file << "\ttime.sleep(600)\n";

	file.close();
}

} // namespace duckdb
