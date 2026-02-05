// transaction.h

// Role: This class is passed around everywhere. It holds:

//     txn_id_t (The unique ID).

//     TransactionState (RUNNING, COMMITTED, ABORTED).

//     write_set_ (List of changes to Undo if it aborts).

//     lock_set_ (List of locks it holds).