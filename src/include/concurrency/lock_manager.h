// lock_manager.h

// Role: Handles Row-level and Table-level locking (2PL).
// Exposes: LockShared(), LockExclusive(), Unlock().
// Consumes: Transaction ID.