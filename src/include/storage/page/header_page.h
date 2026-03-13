// header_page.h

// The Problem: Your Catalog maps table names to Page IDs in memory. But when you restart the database, how does it know that "users" is on Page #5?
// The Fix: You need a special page (usually Page #0) that never moves. It acts as the "root directory," storing the mapping of {"table_name": page_id}.
// API: InsertRecord(name, root_id), GetRootId(name).