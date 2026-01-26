// Pager.h

#pragma once

#include <string>
#include <vector>
#include <cstdint>

using namespace std;


#define PAGE_SIZE 4096



class Pager {
public:
    Pager(const string& filename);
    ~Pager();

    void* GetPage(uint32_t pageNum);
    void Flush(uint32_t pageNum, uint32_t size);
    void FlushAll();
    uint32_t GetUnusedPageNum(); 
public:
    vector<void*> pages;
    uint32_t fileDescriptor;
    uint32_t numPages;
    uint32_t fileLength;
    
};