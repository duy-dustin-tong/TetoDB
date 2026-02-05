// disk_manager.h

// Role: Reads/Writes raw 4KB pages to tetodb.db.
// Exposes: ReadPage(page_id), WritePage(page_id).
// Consumes: OS Filesystem (fstream or unistd.h).