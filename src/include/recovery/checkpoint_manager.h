// checkpoint_manager.h

// What it is: Periodically saves the entire state of the DB so that if it crashes, you don't have to replay history from the beginning of time.
// API: BeginCheckpoint(), EndCheckpoint().