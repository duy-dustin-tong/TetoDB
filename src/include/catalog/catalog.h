// catalog.h

// Role: The master registry.
// Data: Maps table_name -> table_id. Maps table_id -> TableHeap*.
// API:
//     CreateTable(name, schema) -> TableInfo*
//     GetTable(name) -> TableInfo*
//     CreateIndex(name, table_name, keys) -> IndexInfo*