// Pager.cpp

#include "Pager.h"

#include <iostream>
#include <algorithm> // for std::max
#include <cstring>   // for memset
#include <fcntl.h>   // open flags
#include <sys/stat.h> // fstat

#ifdef _WIN32
    #include <io.h>
    #define S_IWUSR S_IWRITE
    #define S_IRUSR S_IREAD
    #define open _open
    #define close _close
    #define read _read
    #define write _write
    #define lseek _lseek
    #define chsize _chsize // For truncating
#else
    #include <unistd.h>
    #define O_BINARY 0
#endif




using namespace std;

Pager::Pager(const string& fileName, uint32_t maxPages)
    : MAX_PAGES(maxPages), fileName(fileName), clockHand(0)
{
    fileDescriptor = open(fileName.c_str(), O_RDWR | O_CREAT | O_BINARY, S_IWUSR | S_IRUSR);

    if(fileDescriptor == -1){
        std::cerr << "Error: Unable to open file " << fileName << std::endl;
        exit(1);
    }

    string tempName = fileName + ".tmp";
    tempFileDescriptor = open(tempName.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_BINARY, S_IWUSR | S_IRUSR);
    if(tempFileDescriptor == -1){
        cerr << "Error: Unable to open temp file " << tempName << endl;
        exit(1);
    }

    struct stat st;
    if(fstat(fileDescriptor, &st) == 0){
        fileLength = st.st_size;
    }
    else{
        fileLength = 0;
    }
    

    buffers.resize(MAX_PAGES);
    for(int i=0; i<MAX_PAGES; i++){
        buffers[i].data = malloc(PAGE_SIZE);
        buffers[i].flags = 0;
    }

    if(fileLength % PAGE_SIZE != 0){
        cerr << "DB file is not a whole number of pages" << endl;
        exit(1);
    }

    numPages = fileLength/PAGE_SIZE;
}

Pager::~Pager(){
    for(PageBuffer &p : buffers) free(p.data);
    close(fileDescriptor);
    close(tempFileDescriptor);
    string tempName = fileName+".tmp";
    if(remove(tempName.c_str()) != 0){
        cerr << "Warning: Could not delete temp file " << tempName << endl;
    }
}


void Pager::WritePage(uint32_t fd, uint32_t pageNum, void* data){
    off_t offset = (off_t)pageNum * PAGE_SIZE;
    lseek(fd, offset, SEEK_SET);
    write(fd, data, PAGE_SIZE);
}

void Pager::ReadPage(uint32_t fd, uint32_t pageNum, void* dest){
    off_t offset = (off_t)pageNum * PAGE_SIZE;
    lseek(fd, offset, SEEK_SET);
    read(fd, dest, PAGE_SIZE);
}

uint16_t Pager::EvictClock() {

    if(pageTable.size() < MAX_PAGES){
        for(uint16_t i=0; i<MAX_PAGES; i++){
            if(!(buffers[i].flags & VALID)) return i;
        }
    }

    while(true){
        PageBuffer& b = buffers[clockHand];

        if(b.flags & RECENT){
            //if is recent, gives second chance
            b.flags ^= RECENT;
            if(++clockHand == MAX_PAGES) clockHand = 0; // advance clock
        } 
        else{
            pageTable.erase(b.pageNum);
            
            if(b.flags & DIRTY){
                WritePage(tempFileDescriptor, b.pageNum, b.data);
                pagesInTemp.insert(b.pageNum);
            }
            
            uint16_t id = clockHand;
            if(++clockHand == MAX_PAGES) clockHand = 0;

            return id;
        }
    }
}

void Pager::MarkDirty(uint32_t pageNum){
    if(pageTable.find(pageNum)!=pageTable.end()){
        uint16_t bufferId = pageTable[pageNum];
        buffers[bufferId].flags |= DIRTY;
    }

}

void* Pager::GetPage(uint32_t pageNum, bool markDirty){
    if(pageNum > numPages){
        cerr << "Error: Tried to fetch page number out of bounds." << endl;
        return nullptr;
    }

    if(pageTable.find(pageNum) != pageTable.end()){
        uint16_t bufferId = pageTable[pageNum];
        PageBuffer &b = buffers[bufferId];
        b.flags |= RECENT; 
        if(markDirty) b.flags |= DIRTY;
        return b.data;
    }

    uint16_t victimId = EvictClock();

    PageBuffer &b = buffers[victimId];

    void* dest = b.data;
    
    b.pageNum = pageNum;
    b.flags = VALID | RECENT;
    if(markDirty) b.flags |= DIRTY;
    
    pageTable[pageNum] = victimId;

    if(pagesInTemp.find(pageNum) != pagesInTemp.end()){
        ReadPage(tempFileDescriptor, pageNum, dest);
        b.flags |= DIRTY;
    }
    else if(pageNum < numPages){
        ReadPage(fileDescriptor, pageNum, dest);
    } 
    else{
        memset(dest, 0, PAGE_SIZE);
        numPages = max(numPages, pageNum + 1);
        b.flags |= DIRTY;
    }


    return dest;
}


void Pager::FlushAll(){
    void* copyBuf = malloc(PAGE_SIZE);

    for(uint32_t pageNum : pagesInTemp){
        if(pageTable.find(pageNum) == pageTable.end()){
            ReadPage(tempFileDescriptor, pageNum, copyBuf);
            WritePage(fileDescriptor, pageNum, copyBuf);
        }
    }
    free(copyBuf);

    for(PageBuffer& b : buffers) if((b.flags & VALID) && (b.flags & DIRTY)){
        WritePage(fileDescriptor, b.pageNum, b.data);
        b.flags &= ~DIRTY;
    }

    #ifdef _WIN32
        _commit(fileDescriptor);
    #else
        fsync(fileDescriptor);
    #endif

    #ifdef _WIN32
        _chsize(tempFileDescriptor, 0); 
    #else
        ftruncate(tempFileDescriptor, 0);
    #endif
    
    pagesInTemp.clear();
}



