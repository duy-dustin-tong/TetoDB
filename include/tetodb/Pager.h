// Pager.h

#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <bitset> 
#include <list>

using namespace std;


#define PAGE_SIZE 4096
const uint32_t DEFAULT_CACHE_LIMIT = 50000; //50k page ~200MB

enum PageBufferFlags : uint8_t {
    VALID = 1,
    DIRTY = 2,
    RECENT = 4
};

struct PageBuffer{
    void* data;          
    uint32_t pageNum;    
    uint8_t flags; 
};

class Pager {
public:
    Pager(const string& filename, uint32_t maxPages = DEFAULT_CACHE_LIMIT);
    ~Pager();

    
    // Core I/O Helpers
    void WritePage(uint32_t fd, uint32_t pageNum, void* data);
    void ReadPage(uint32_t fd, uint32_t pageNum, void* dest);
    
    // Management
    void* GetPage(uint32_t pageNum, bool markDirty);
    void MarkDirty(uint32_t pageNum);
    uint16_t EvictClock();
    void FlushAll(); // COMMIT

    
public:
    string fileName;



    uint32_t MAX_PAGES;
    vector<PageBuffer> buffers;
    unordered_map<uint32_t, uint16_t> pageTable; // maps pageId -> index in buffers
    unordered_set<uint32_t> pagesInTemp;
    uint16_t clockHand; 

    uint32_t fileDescriptor;
    uint32_t tempFileDescriptor;

    uint32_t numPages;
    uint32_t fileLength;
};