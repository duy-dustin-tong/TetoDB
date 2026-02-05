// binder.h

// Role: The "Type Checker". It takes the Parser's AST and checks the Catalog.
// Job: It turns the string "users" into the actual table_oid_t #5. It throws errors like "Column does not exist."