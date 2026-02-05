// teto_db.h

// Role: The main library class.
// Exposes:
//     Status ExecuteSql(std::string sql, ResultSet &result);
//     Transaction BeginTransaction();
// Consumes: Everything below it (Parser, Optimizer, Execution, Storage).