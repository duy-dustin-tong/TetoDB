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

struct LeafCell{
    int32_t key;
    uint32_t rowId;
};

struct InternalCell{
    int32_t key;
    uint32_t rowId;
    uint32_t childPage;
};

struct LeafNode{
    NodeHeader header;
    uint32_t nextLeaf;
    LeafCell cells[0];
};

struct InternalNode{
    NodeHeader header;
    uint32_t rightChild;
    InternalCell cells[0];
};

struct InsertResult{
    bool success;
    bool didSplit;
    int32_t splitKey;
    uint32_t splitRowId;
    uint32_t rightChildPageNum;
};

void CreateNewRoot(NodeHeader* root, Pager* pager, int32_t splitKey, uint32_t splitRowId, uint32_t rightChildPageNum);
void InitializeLeafNode(LeafNode* node);
bool LeafNodeInsertNonFull(Table* t, LeafNode* node, int32_t key, uint32_t rowId);
InsertResult LeafNodeInsert(Table* t, LeafNode* node, Pager* pager, int32_t key, uint32_t rowId);
InsertResult InternalNodeInsert(InternalNode* node, Pager* pager, int32_t key, uint32_t rowId, uint32_t rigthChildPage);
uint16_t LeafNodeFindSlot(LeafNode* node, int32_t targetKey, uint32_t targetRowId);
uint32_t InternalNodeFindChild(InternalNode* node, int32_t targetKey, uint32_t targetRowId);
uint32_t BtreeFindLeaf(Pager* pager, uint32_t pageNum, int32_t key, uint32_t rowId);
void LeafNodeSelectRange(Table* t, LeafNode* node, int32_t L, int32_t R, vector<uint32_t>& outRowIds);
void UpdateChildParents(Pager* pager, InternalNode* parentNode, uint32_t parentPageNum);
void InsertIntoParent(Pager* pager, NodeHeader* leftChild, int32_t key, uint32_t rowId, uint32_t rightChildPageNum);
uint32_t BtreeDelete(Table* t, Pager* pager, int32_t L, int32_t R);
uint16_t LeafNodeDeleteRange(Table* t, LeafNode* node, int32_t L, int32_t R);

//const uint32_t NODE_SIZE = 4096;
const uint32_t LEAF_NODE_SIZE = 4096;
const uint32_t INTERNAL_NODE_SIZE = 4096;
const uint32_t HEADER_SIZE = sizeof(NodeHeader);
const uint32_t LEAF_CELL_SIZE = sizeof(LeafCell);
const uint32_t INTERNAL_CELL_SIZE = sizeof(InternalCell);

const uint32_t LEAF_NODE_MAX_CELLS = 3;//(NODE_SIZE - HEADER_SIZE) / CELL_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;// (NODE_SIZE - sizeof(InternalNode)) / sizeof(InternalCell);
