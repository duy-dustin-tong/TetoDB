// execution_context.h

// Role: A "Context Object" passed to every Executor.
// Why: When an Executor runs, it needs access to the LockManager, BufferPool, and Transaction. Instead of passing 5 arguments to every function, you pass this one context object.