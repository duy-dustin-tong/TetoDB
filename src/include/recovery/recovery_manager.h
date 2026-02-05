// recovery_manager.h

// What it is: The engine that runs only when the database starts up after a crash. It reads the logs and fixes the data files.
// API: ARIES(): Runs the Analysis, Redo, and Undo phases.