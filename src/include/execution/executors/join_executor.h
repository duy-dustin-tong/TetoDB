// join_executor.h

// Role: Handles JOIN. For every row in A, scan B.
// Exposes: Next().
// Consumes: Two child executors (Left and Right).