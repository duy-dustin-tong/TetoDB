// Btree.h
#pragma once

#include <cstdint>
#include <vector>

using namespace std;

class Pager;
class Table;


enum NodeType : uint8_t { INTERNAL, LEAF };

struct NodeHeader{
    NodeType type;
    uint8_t isRoot;
    uint16_t numCells;
    int32_t parent;
};

template<typename T>
struct LeafCell{
    T key;
    uint32_t rowId;
};

template<typename T>
struct InternalCell{
    T key;
    uint32_t rowId;
    uint32_t childPage;
};

template<typename T>
struct LeafNode{
    NodeHeader header;
    uint32_t nextLeaf;
    LeafCell<T> cells[0];
};

template<typename T>
struct InternalNode{
    NodeHeader header;
    uint32_t rightChild;
    InternalCell<T> cells[0];
};

template<typename T>
struct InsertResult{
    bool success;
    bool didSplit;
    T splitKey;
    uint32_t splitRowId;
    uint32_t rightChildPageNum;
};


template<typename T>
class Btree{

public:
    Btree(Pager* p, Table* t);
    ~Btree();

    void CreateIndex();

    InsertResult<T> Insert(T key, uint32_t rowId);
    void SelectRange(T L, T R, vector<uint32_t>& outRowIds);
    uint32_t DeleteRange(T L, T R);


    uint32_t FindLeaf(uint32_t pageNum, T key, uint32_t rowId);

private:
    void CreateNewRoot(NodeHeader* root, T splitKey, uint32_t splitRowId, uint32_t rightChildPageNum);
    void InitializeLeafNode(LeafNode<T>* node);

    uint32_t InternalNodeFindChild(InternalNode<T>* node, T targetKey, uint32_t targetRowId);
    uint16_t LeafNodeFindSlot(LeafNode<T>* node, T targetKey, uint32_t targetRowId);

    InsertResult<T> InternalNodeInsert(InternalNode<T>* node, T key, uint32_t rowId, uint32_t rightChildPage);
    InsertResult<T> LeafNodeInsert(LeafNode<T>* node, T key, uint32_t rowId);

    bool LeafNodeInsertNonFull(LeafNode<T>* node, T key, uint32_t rowId);
    

    void InsertIntoParent(NodeHeader* leftChild, T key, uint32_t rowId, uint32_t rightChildPageNum);
    void UpdateChildParents(InternalNode<T>* parentNode, uint32_t parentPageNum);

    void LeafNodeSelectRange(LeafNode<T>* node, T L, T R, vector<uint32_t>& outRowIds);
    uint16_t LeafNodeDeleteRange(LeafNode<T>* node, T L, T R);
    



public:
    Pager* pager;
    Table* table;
    uint32_t rootPageNum;


public:
    inline static const uint32_t LEAF_NODE_SIZE = 4096;
    inline static const uint32_t INTERNAL_NODE_SIZE = 4096;
    inline static const uint32_t HEADER_SIZE = sizeof(NodeHeader);
    inline static const uint32_t LEAF_CELL_SIZE = sizeof(LeafCell<T>);
    inline static const uint32_t INTERNAL_CELL_SIZE = sizeof(InternalCell<T>);

    inline static const uint32_t LEAF_NODE_MAX_CELLS = (LEAF_NODE_SIZE - sizeof(LeafNode<T>)) / LEAF_CELL_SIZE;
    inline static const uint32_t INTERNAL_NODE_MAX_CELLS = (INTERNAL_NODE_SIZE - sizeof(InternalNode<T>)) / INTERNAL_CELL_SIZE;
};

#include "Btree.tpp"



