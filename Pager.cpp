// Pager.cpp

#include "Pager.h"

#include <iostream>
#include <algorithm> // for std::max
#include <cstring>   // for memset
#include <fcntl.h>   // open flags
#include <sys/stat.h> // fstat

#ifdef _WIN32
    #include <io.h>  // Windows equivalent of unistd.h
    #define F_OK 0
    // Windows permissions
    #define S_IWUSR S_IWRITE
    #define S_IRUSR S_IREAD
    #include <cstddef>
    using ssize_t = ptrdiff_t;
#else
    #include <unistd.h> // Linux/Mac standard
#endif

#ifndef O_BINARY
#define O_BINARY 0
#endif




using namespace std;

Pager::Pager(const std::string& filename){
    fileDescriptor = open(filename.c_str(), O_RDWR | O_CREAT | O_BINARY, S_IWUSR | S_IRUSR);

    if(fileDescriptor == -1){
        std::cerr << "Error: Unable to open file " << filename << std::endl;
        exit(1);
    }

    struct stat st;
    if(fstat(fileDescriptor, &st) == 0){
        fileLength = st.st_size;
    }
    else{
        fileLength = 0;
    }


    pages.clear();

    if(fileLength % PAGE_SIZE != 0){
        cerr << "DB file is not a whole number of pages" << endl;
        exit(1);
    }

    numPages = fileLength/PAGE_SIZE;
    pages.resize(numPages, nullptr);
}

Pager::~Pager(){
    for(uint32_t i = 0;i<numPages; i++) free(pages[i]);
    close(fileDescriptor);
}

void* Pager::GetPage(uint32_t pageNum){
    if(pageNum > numPages){
        std::cerr << "Error: Tried to fetch page number out of bounds." << std::endl;
        return nullptr;
    }

    if(pageNum == numPages) numPages++, pages.push_back(nullptr);

    if(pages[pageNum] != nullptr){
        return pages[pageNum];
    }

    void* page = malloc(PAGE_SIZE);
    uint32_t numPagesStored = fileLength / PAGE_SIZE;

    if(pageNum <= numPagesStored){
        lseek(fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
        ssize_t bytesRead = read(fileDescriptor, page, PAGE_SIZE);
        
        if(bytesRead == -1){
            cerr << "Error reading file: " << errno << std::endl;
            exit(1);
        }
    }

    pages[pageNum] = page;
    return pages[pageNum];
}

void Pager::Flush(uint32_t pageNum, uint32_t size){
    if(pages[pageNum] == nullptr) return;

    off_t offset = lseek(fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
    if(offset == -1){
        cerr << "Error seeking file." << std::endl;
        return;
    }

    ssize_t bytesWritten = write(fileDescriptor, pages[pageNum], size);
    if(bytesWritten == -1){
        cerr << "Error writing to file." << std::endl;
        return;
    }

    fileLength = max(fileLength, (uint32_t)(pageNum + 1) * PAGE_SIZE);
    #ifdef _WIN32
        _commit(fileDescriptor); // Windows commit
    #else
        fsync(fileDescriptor);   // Linux commit
    #endif
}

void Pager::FlushAll(){
    for(uint32_t i = 0; i < numPages; i++){
        if(pages[i]) Flush(i, PAGE_SIZE);
    }
}

uint32_t Pager::GetUnusedPageNum(){
    pages.push_back(nullptr);
    return numPages++;
}

