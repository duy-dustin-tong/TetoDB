// log_manager.h

// What it is: The active worker that writes log records to the disk before the actual data page is touched (Write-Ahead Logging).
// API:
//     AppendLogRecord(LogRecord &record): Adds a new entry to the buffer.
//     Flush(bool force): Pushes the log buffer to the DiskManager.
// Consumes: DiskManager.