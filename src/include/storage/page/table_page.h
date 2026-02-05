// table_page.h

// Role: The raw Page class (in your structure) is just a byte array. TablePage is a specialized wrapper that knows how to format those bytes into Slots and Tuples.
// API: InsertTuple, GetTuple, MarkDeleted.