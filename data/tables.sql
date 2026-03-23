create table sidra_clients(id ubigint primary key, creation timestamp, last_update timestamp);
create table sidra_settings(setting varchar primary key, value varchar);
create table sidra_tables(name varchar primary key, type tinyint, query varchar, is_view boolean);
create table sidra_clients_refreshes(id ubigint references sidra_clients(id), table_name varchar references sidra_tables(name), last_result timestamp, last_refresh timestamp);
create table sidra_table_constraints(table_name varchar references sidra_tables(name), column_name varchar, sidra_sensitive bool, sidra_fact bool, sidra_dimension bool, primary key(table_name, column_name));
create table sidra_view_constraints(view_name varchar references sidra_tables(name), sidra_window int, sidra_ttl tinyint, sidra_refresh tinyint, sidra_min_agg tinyint, last_refresh timestamp, primary key(view_name));
create table sidra_current_window(view_name varchar references sidra_tables(name), sidra_window int, last_update timestamp, primary key(view_name));
