#include <boost/describe.hpp>
#include <boost/mp11.hpp>
#include <fmt/format.h>
#include <type_traits>
#include <SQLiteCpp/SQLiteCpp.h>
#include <iostream>

export module bdsqlite;

export namespace bdsqlite {

inline std::string insert_value(int v) { return fmt::format("{0},", v); }
inline std::string insert_value(float v) { return fmt::format("{0},", v); }
inline std::string insert_value(double v) { return fmt::format("{0},", v); }
inline std::string insert_value(const std::string& v) { return fmt::format("\"{0}\",", v); }
inline std::string insert_value(const std::chrono::system_clock::time_point& v) { return fmt::format("{0},", std::chrono::duration_cast<std::chrono::seconds>(v.time_since_epoch()).count()); }

inline std::string insert_value(const std::optional<int>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL,"; } }
inline std::string insert_value(const std::optional<float>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL,"; } }
inline std::string insert_value(const std::optional<double>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL,"; } }
inline std::string insert_value(const std::optional<std::string>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL,"; } }
inline std::string insert_value(const std::optional<std::chrono::system_clock::time_point>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL,"; } }

void get_cell(int& des, const SQLite::Column& v) { des = v.getInt(); }
void get_cell(float& des, const SQLite::Column& v) { des = v.getDouble(); }
void get_cell(double& des, const SQLite::Column& v) { des = v.getDouble(); }
void get_cell(std::string& des, const SQLite::Column& v) { des = v.getString(); }
void get_cell(std::chrono::system_clock::time_point& des, const SQLite::Column& v) { des = std::chrono::system_clock::from_time_t(time_t{ 0 }) + std::chrono::seconds(v.getInt()); }

void get_cell(std::optional<int>& des, const SQLite::Column& v) { if (!v.isNull()) { des = v.getInt(); } }
void get_cell(std::optional<float>& des, const SQLite::Column& v) { if (!v.isNull()) { des = v.getDouble(); } }
void get_cell(std::optional<double>& des, const SQLite::Column& v) { if (!v.isNull()) { des = v.getDouble(); } }
void get_cell(std::optional<std::string>& des, const SQLite::Column& v) { if (!v.isNull()) { des = v.getString(); } }
void get_cell(std::optional<std::chrono::system_clock::time_point>& des, const SQLite::Column& v) { if (!v.isNull()) { des = std::chrono::system_clock::from_time_t(time_t{ 0 }) + std::chrono::seconds(v.getInt()); } }


template<typename T>
inline std::vector<std::string> column_names() {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;

	std::vector<std::string> ret;

	boost::mp11::mp_for_each<Md>([&](auto D) {
		std::string colname = D.name;
	if (colname.back() == '_') colname.pop_back();
	ret.push_back(colname);
		});
	return ret;
}

template<typename T>
void read_row(T& t, SQLite::Statement& row) {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;
	int i = 0;
	boost::mp11::mp_for_each<Md>([&](auto D) {
		get_cell(t.*D.pointer, row.getColumn(i));
	i++;
		});
}

template<typename T>
inline std::string GetCreateTableQuery(const std::string& tableName, const std::string& primaryKey) {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;

	std::string query = fmt::format("CREATE TABLE if not exists {0} (", tableName);

	boost::mp11::mp_for_each<Md>([&](auto D) {
		std::string colname = D.name;
	if (colname.back() == '_') colname.pop_back();
	query += colname;
	std::string fullname = typeid(D.pointer).name();
	std::string coltype;

	if (fullname.rfind("int", 0) == 0) { coltype = "INTEGER"; }
	if (fullname.rfind("float", 0) == 0) { coltype = "REAL"; }
	if (fullname.rfind("double", 0) == 0) { coltype = "REAL"; }
	if (fullname.rfind("class std::basic_string", 0) == 0) { coltype = "TEXT"; }
	if (fullname.rfind("class std::chrono::time_point", 0) == 0) { coltype = "INTEGER"; }

	if (fullname.rfind("class std::optional<int>", 0) == 0) { coltype = "INTEGER"; }
	if (fullname.rfind("class std::optional<float>", 0) == 0) { coltype = "REAL"; }
	if (fullname.rfind("class std::optional<double>", 0) == 0) { coltype = "REAL"; }
	if (fullname.rfind("class std::optional<class std::basic_string", 0) == 0) { coltype = "TEXT"; }
	if (fullname.rfind("class std::optional<class std::chrono::time_point", 0) == 0) { coltype = "INTEGER"; }

	query += " " + coltype + ",";
		});
	query += "PRIMARY KEY (" + primaryKey + "))";
	query += ";";
	return query;
}

template<typename T>
inline void CreateTableIfNotExists(SQLite::Database& db, const std::string& tableName, const std::string& primaryKey) {
	std::string query;
	try {
		query = GetCreateTableQuery<T>(tableName, primaryKey);
		db.exec(query);
	}
	catch (std::exception e) {
		std::cout << e.what() << "\n" << "error from CreateTableI:\n";
		std::cout << query << "\n";
	}
}

template<typename T>
inline std::string ToInsertString(const T& t, const std::string& tableName, const std::string& insertType = "INSERT INTO") {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;

	std::string query = fmt::format("{0} {1} VALUES(", insertType, tableName);
	boost::mp11::mp_for_each<Md>([&](auto D) {
		query += insert_value(t.*D.pointer);
		});
	query.pop_back();
	query += ");";
	return query;
}

template<typename T>
inline void ReplaceInto(SQLite::Database& db, const std::string& tableName, const T& value) {
	std::string query;
	try {
		query = ToInsertString(value, tableName, "REPLACE INTO");
		db.exec(query);
	}
	catch (std::exception e) {
		std::cout << e.what() << "\n" << "error from ReplaceInto:\n";
		std::cout << query << "\n";
	}
}

template<typename T>
std::vector<T> Select(SQLite::Database& db, const std::string& tableName, const std::string& condition = "") {
	std::vector<std::string> names = column_names<T>();
	std::string selects = "SELECT ";
	for (auto& name : names) {
		selects += name + ",";
	}
	selects.pop_back();
	auto query = fmt::format("{0} FROM {1}", selects, tableName);
	if (!condition.empty()) { query += " WHERE " + condition; }

	std::vector<T> ret;

	SQLite::Statement stmt(db, query);
	while (stmt.executeStep()) {
		T object;
		read_row(object, stmt);
		ret.push_back(object);
	}
	return ret;
}

}
