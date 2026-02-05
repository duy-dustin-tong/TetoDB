// log_record.h

// What it is: Defines the data structure for a single "event" in the database (e.g., "Transaction #5 changed value 'A' from 10 to 20").
// Components: LogRecordType (INSERT, UPDATE, BEGIN, COMMIT), LSN (Log Sequence Number), PrevLSN.
// API: Serialize(), Deserialize()