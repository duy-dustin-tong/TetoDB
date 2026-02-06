#include <iostream>
#include <vector>
#include <cstring>
#include <cassert>
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/buffer/two_queue_replacer.h"

using namespace tetodb;

// --- Helper: Fill a page with distinct numbers ---
void WriteRandomData(Page* page, int seed) {
    int* data = reinterpret_cast<int*>(page->GetData());
    int num_ints = PAGE_SIZE / sizeof(int);
    for (int i = 0; i < num_ints; i++) {
        data[i] = seed + i;
    }
}

// --- Helper: Verify the numbers match ---
bool CheckRandomData(Page* page, int seed) {
    int* data = reinterpret_cast<int*>(page->GetData());
    int num_ints = PAGE_SIZE / sizeof(int);
    for (int i = 0; i < num_ints; i++) {
        if (data[i] != seed + i) return false;
    }
    return true;
}

int main() {
    // 0. Clean up previous runs
    remove("test.db");
    std::cout << "=== TetoDB Storage Kernel Test ===" << std::endl;

    DiskManager disk("test.db");
    TwoQueueReplacer replacer(5); // Small pool (5 pages) to force eviction

    // We store Page IDs here to verify them later
    std::vector<page_id_t> page_ids;

    // --- SESSION SCOPE START ---
    {
        BufferPoolManager bpm(5, &disk, &replacer);

        // ---------------------------------------------------------
        // TEST A: Allocation & Write
        // ---------------------------------------------------------
        std::cout << "[Test A] Allocating 5 Pages..." << std::endl;
        for (int i = 0; i < 5; i++) {
            page_id_t pid;
            Page* p = bpm.NewPage(&pid);
            assert(p != nullptr);

            // Write data specific to this page ID
            WriteRandomData(p, pid * 1000);
            page_ids.push_back(pid);

            // Unpin immediately. Mark DIRTY (true) so it saves later.
            bpm.UnpinPage(pid, true);
        }
        std::cout << "SUCCESS: Allocated 5 pages. Pool is full." << std::endl;

        // ---------------------------------------------------------
        // TEST B: Eviction (2Q Policy)
        // ---------------------------------------------------------
        std::cout << "[Test B] Forcing Eviction (Allocating Page 6)..." << std::endl;
        page_id_t overflow_pid;
        Page* p_overflow = bpm.NewPage(&overflow_pid);

        if (p_overflow != nullptr) {
            std::cout << "SUCCESS: Eviction triggered! New Page ID: " << overflow_pid << std::endl;
            WriteRandomData(p_overflow, 9999);
            bpm.UnpinPage(overflow_pid, true);
            page_ids.push_back(overflow_pid);
        }
        else {
            std::cout << "FAIL: Eviction failed. Returned nullptr." << std::endl;
            return 1;
        }

        // ---------------------------------------------------------
        // TEST C: Read Back (Round-Trip)
        // ---------------------------------------------------------
        std::cout << "[Test C] Reading back evicted data (Page 0)..." << std::endl;
        // Page 0 was likely evicted to make room for Page 5. 
        // Calling FetchPage forces BPM to read it from Disk.
        Page* p0 = bpm.FetchPage(page_ids[0]);
        assert(p0 != nullptr);

        if (CheckRandomData(p0, page_ids[0] * 1000)) {
            std::cout << "SUCCESS: Page 0 data survived disk round-trip!" << std::endl;
        }
        else {
            std::cout << "FAIL: Corrupt data on Page 0." << std::endl;
            return 1;
        }
        bpm.UnpinPage(page_ids[0], false);

        // ---------------------------------------------------------
        // TEST D: Transaction Commit (Manual Flush)
        // ---------------------------------------------------------
        std::cout << "[Transaction] Committing (Flushing all pages)..." << std::endl;
        bpm.FlushAllPages(); // <--- This is the key line for your session architecture

    }
    // --- SESSION SCOPE END --- 
    // The BPM is destroyed here. Since we removed the destructor flush, 
    // correctness depends entirely on the FlushAllPages() call above.

    // ---------------------------------------------------------
    // TEST E: Verify Persistence
    // ---------------------------------------------------------
    std::cout << "[Test E] Verifying physical file size..." << std::endl;
    int size = disk.GetFileSize();
    int expected = page_ids.size() * PAGE_SIZE; // Should be 6 * 4096 = 24576 bytes

    if (size >= expected) {
        std::cout << "SUCCESS: File size is " << size << " bytes." << std::endl;
    }
    else {
        std::cout << "FAIL: File size is " << size << " (Expected >= " << expected << ")" << std::endl;
        return 1;
    }

    std::cout << "=== ALL TESTS PASSED ===" << std::endl;
    return 0;
}