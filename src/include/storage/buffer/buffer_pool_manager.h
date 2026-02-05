// buffer_pool_manager.h

// Role: The Cache. Moves pages from Disk <-> Memory. (You are building this now!).
// Exposes: FetchPage(), UnpinPage(), FlushPage().
// Consumes: DiskManager, LRUKReplacer.