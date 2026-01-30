// Btree.cpp

#include "Btree.h"
#include "Pager.h"  // Now we need the full definition
#include "Schema.h" // Now we need Table definition (IsRowDeleted)
#include "Common.h"

#include <cstring>
#include <algorithm> // for memmove


void CreateNewRoot(NodeHeader* root, Pager* pager, int32_t splitKey, uint32_t splitRowId, uint32_t rightChildPageNum){
    pager->MarkDirty(0);

    uint32_t leftChildPageNum = pager->numPages;
    NodeHeader* leftChild = (NodeHeader*)pager->GetPage(leftChildPageNum, 1);

    memcpy(leftChild, root, INTERNAL_NODE_SIZE);
    leftChild->isRoot = 0;
    leftChild->parent = 0;

    NodeHeader* rightChild = (NodeHeader*)pager->GetPage(rightChildPageNum, 1);
    rightChild->parent = 0;

    InternalNode* internalRoot = (InternalNode*)root;
    internalRoot->header.type = INTERNAL;
    internalRoot->header.isRoot = 1;
    internalRoot->header.numCells = 1;
    internalRoot->header.parent = 0;

    internalRoot->rightChild = rightChildPageNum;

    internalRoot->cells[0].key = splitKey;
    internalRoot->cells[0].rowId = splitRowId;
    internalRoot->cells[0].childPage = leftChildPageNum;

    if(leftChild->type == INTERNAL) UpdateChildParents(pager, (InternalNode*)leftChild, leftChildPageNum);
    if(rightChild->type == INTERNAL) UpdateChildParents(pager, (InternalNode*)rightChild, rightChildPageNum);
}

void InitializeLeafNode(LeafNode* node){
    node->header.type = LEAF;
    node->header.isRoot = 0;
    node->header.numCells = 0;
    node->header.parent = 0;


    node->nextLeaf = 0;

    memset(node->cells, 0, LEAF_NODE_SIZE - HEADER_SIZE);
}

uint16_t LeafNodeFindSlot(LeafNode* node, int32_t targetKey, uint32_t targetRowId){
    uint16_t l = 0;
    uint16_t r = node->header.numCells;

    while(l<r){
        uint16_t mid = l+r>>1;
        int32_t midKey = node->cells[mid].key;
        uint32_t midRowId = node->cells[mid].rowId;
        if(targetKey < midKey || (targetKey == midKey && targetRowId < midRowId)) r = mid;
        else l = mid+1;
    }

    return l;
}

uint32_t InternalNodeFindChild(InternalNode* node, int32_t targetKey, uint32_t targetRowId){
    uint16_t l = 0;
    uint16_t r = node->header.numCells;

    while(l<r){
        uint16_t mid = l+r>>1;
        int32_t midKey = node->cells[mid].key;
        uint32_t midRowId = node->cells[mid].rowId;
        if(targetKey < midKey || (targetKey == midKey && targetRowId < midRowId)) r=mid;
        else l = mid+1;
    }

    if(l == node->header.numCells) return node->rightChild;
    return node->cells[l].childPage;
}

uint32_t BtreeFindLeaf(Pager* pager, uint32_t pageNum, int32_t key, uint32_t rowId){
    void* node = pager->GetPage(pageNum, 0);
    NodeHeader* header = (NodeHeader*) node;

    if(header->type==LEAF) return pageNum;

    InternalNode* internal = (InternalNode*) node;
    uint32_t childPageNum = InternalNodeFindChild(internal, key, rowId);
    
    return BtreeFindLeaf(pager, childPageNum, key, rowId); 
}

void UpdateChildParents(Pager* pager, InternalNode* parentNode, uint32_t parentPageNum){
    void* child = pager->GetPage(parentNode->rightChild, 1);
    ((NodeHeader*)child)->parent = parentPageNum;

    for(uint16_t i = 0; i<parentNode->header.numCells;i++){
        void* child = pager->GetPage(parentNode->cells[i].childPage, 1);
        ((NodeHeader*)child)->parent = parentPageNum;
    }
}

void InsertIntoParent(Pager* pager, NodeHeader* leftChild, int32_t key, uint32_t rowId, uint32_t rightChildPageNum){
    uint32_t parentPageNum = leftChild->parent;

    if(parentPageNum == 0){
        InternalNode* root = (InternalNode*)pager->GetPage(0,1);

        InsertResult res = InternalNodeInsert(root, pager, key, rowId, rightChildPageNum);
        if(!res.didSplit) return;

        CreateNewRoot((NodeHeader*)root, pager, res.splitKey, res.splitRowId, res.rightChildPageNum);


        return;
    }

    InternalNode* parent = (InternalNode*)pager->GetPage(parentPageNum, 1);
    InsertResult res = InternalNodeInsert(parent, pager, key, rowId, rightChildPageNum);

    if(res.didSplit){
        InsertIntoParent(pager, (NodeHeader*) parent, res.splitKey, res.splitRowId, res.rightChildPageNum);
    }
}

bool LeafNodeInsertNonFull(Table* t, LeafNode* node, int32_t key, uint32_t rowId){
    uint16_t slot = LeafNodeFindSlot(node, key, rowId);
    bool isDeleted = t->IsRowDeleted(node->cells[slot].rowId);

    if(node->header.numCells >= LEAF_NODE_MAX_CELLS && !isDeleted) return 0;

    if(!isDeleted){
        uint16_t cellsToMove = node->header.numCells - slot;
        if(cellsToMove > 0){
            void* src = &node->cells[slot];
            void* dest = &node->cells[slot+1];
            memmove(dest, src, cellsToMove*LEAF_CELL_SIZE);
        }
        node->header.numCells++;
    }

    node->cells[slot].key = key;
    node->cells[slot].rowId = rowId;  
    return 1;
}

InsertResult LeafNodeInsert(Table* t, LeafNode* node, Pager* pager, int32_t key, uint32_t rowId){

    if(LeafNodeInsertNonFull(t, node, key, rowId)) return {true, false, 0, 0, 0};


    uint32_t newPageNum = pager->numPages;
    
    LeafNode* rightNode = (LeafNode*) pager->GetPage(newPageNum, 1);
    InitializeLeafNode(rightNode);
    rightNode->header.isRoot = 0;
    rightNode->header.parent = node->header.parent;

    rightNode->nextLeaf = node->nextLeaf;
    node->nextLeaf = newPageNum;

    uint16_t splitIdx = (LEAF_NODE_MAX_CELLS+1)/2;
    uint16_t cellsMoved = node->header.numCells-splitIdx;

    void* src = &node->cells[splitIdx];
    void* dest = &rightNode->cells[0];
    memcpy(dest, src, cellsMoved*LEAF_CELL_SIZE);

    node->header.numCells = splitIdx;
    rightNode->header.numCells = cellsMoved;

    int32_t splitKey = rightNode->cells[0].key;
    uint32_t splitRowId = rightNode->cells[0].rowId;

    if(key>=splitKey) LeafNodeInsertNonFull(t, rightNode, key, rowId);
    else LeafNodeInsertNonFull(t, node, key, rowId);

    

    return {true, true, splitKey, splitRowId, newPageNum};

}

InsertResult InternalNodeInsert(InternalNode* node, Pager* pager, int32_t key, uint32_t rowId, uint32_t rightChildPage){
    if(node->header.numCells < INTERNAL_NODE_MAX_CELLS){
        uint16_t i = 0;
        while(i < node->header.numCells){
            bool targetIsSmaller = (key < node->cells[i].key) || (key == node->cells[i].key && rowId < node->cells[i].rowId);
            if(targetIsSmaller) break;
            i++;
        }

        for(uint16_t j = node->header.numCells; j > i; j--){
            node->cells[j] = node->cells[j-1];
        }
        
        if(i == node->header.numCells){
            node->cells[i].key = key;
            node->cells[i].rowId = rowId;
            node->cells[i].childPage = node->rightChild;
            node->rightChild = rightChildPage;
        }
        else{
            node->cells[i+1].childPage = rightChildPage;
            node->cells[i].key = key;
            node->cells[i].rowId = rowId;
        }

        node->header.numCells++;

        return {true, false, 0, 0, 0};
    }

    uint32_t newPageNum = pager->numPages;
    InternalNode* rightNode = (InternalNode*)pager->GetPage(newPageNum, 1);

    rightNode->header.type = INTERNAL;
    rightNode->header.isRoot = 0;
    rightNode->header.numCells = 0;
    rightNode->header.parent = node->header.parent;

    uint16_t splitIdx = INTERNAL_NODE_MAX_CELLS/2;

    int32_t promotedKey = node->cells[splitIdx].key;
    uint32_t promotedRowId = node->cells[splitIdx].rowId;

    uint32_t leftNewRightChild = node->cells[splitIdx].childPage;

    uint16_t cellsMoved = node->header.numCells - splitIdx - 1;

    memcpy(rightNode->cells, &node->cells[splitIdx+1], cellsMoved*sizeof(InternalCell));

    rightNode->header.numCells = cellsMoved;

    rightNode->rightChild = node->rightChild;
    node->rightChild = leftNewRightChild;
    node->header.numCells = splitIdx;


    if(key > promotedKey || (key==promotedKey && rowId > promotedRowId)){
        InternalNodeInsert(rightNode, pager, key, rowId, rightChildPage);
    }
    else{
        InternalNodeInsert(node, pager, key, rowId, rightChildPage);
    }

    UpdateChildParents(pager, rightNode, newPageNum);

    return {true, true, promotedKey, promotedRowId, newPageNum};
}

void LeafNodeSelectRange(Table* t, LeafNode* node, int32_t L, int32_t R, vector<uint32_t>& outRowIds){
    uint16_t p = 0;
    for(uint16_t q = 0;q<node->header.numCells;q++){
        uint32_t rowId = node->cells[q].rowId;
        if(t->IsRowDeleted(rowId)) continue;

        int32_t key = node->cells[q].key;
        if(L<=key && key<=R) outRowIds.push_back(rowId);

        if(p!=q) memcpy(&node->cells[p], &node->cells[q], LEAF_CELL_SIZE);
        p++;
    }

    node->header.numCells = p;

}

uint16_t LeafNodeDeleteRange(Table* t, LeafNode* node, int32_t L, int32_t R){
    uint16_t p = 0;
    uint16_t deletedCount = 0;
    for(uint16_t q = 0;q<node->header.numCells;q++){
        uint32_t rowId = node->cells[q].rowId;
        if(t->IsRowDeleted(rowId)) continue;

        int32_t key = node->cells[q].key;
        if(L<=key && key<=R){
            t->MarkRowDeleted(rowId);
            deletedCount++;
            continue;
        }

        if(p!=q) memcpy(&node->cells[p], &node->cells[q], LEAF_CELL_SIZE);
        p++;
    }
    
    node->header.numCells = p;

    return deletedCount;
}

uint32_t BtreeDelete(Table* t, Pager* pager, int32_t L, int32_t R){
    uint32_t leafPageNum = BtreeFindLeaf(pager,0,L,0);
    uint32_t deletedCount = 0;
    bool firstPage = 1;
    
    while(leafPageNum != 0 || (firstPage && leafPageNum == 0)){
        LeafNode* leaf = (LeafNode*)pager->GetPage(leafPageNum, 1); // mat change to not mark dirty later
        deletedCount += LeafNodeDeleteRange(t, leaf, L, R);

        if(leaf->header.numCells > 0){
            int lastKey = leaf->cells[leaf->header.numCells - 1].key;
            if(lastKey > R) break;
        }
        firstPage = 0;
        leafPageNum = leaf->nextLeaf;
        //pager->Flush(leafPageNum, PAGE_SIZE);
    }

    return deletedCount;

}

