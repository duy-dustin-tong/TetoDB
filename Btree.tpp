// Btree.tpp

#include "Pager.h"  
#include "Schema.h" 
#include "Common.h"

#include <cstring>
#include <algorithm> // for memmove

template<typename T>
Btree<T>::Btree(Pager* p, Table* t)
    : pager(p), table(t), rootPageNum(0)
{

}

template<typename T>
Btree<T>::~Btree(){

}

template<typename T>
void Btree<T>::CreateIndex(){
    LeafNode<T>* root = (LeafNode<T>*) pager->GetPage(rootPageNum, 1);
    InitializeLeafNode(root);
    root->header.isRoot = 1;
}

template<typename T>
InsertResult<T> Btree<T>::Insert(T key, uint32_t rowId){
    uint32_t leafPageNum = FindLeaf(rootPageNum, key, rowId);
    LeafNode<T>* leaf = pager->GetPage(leafPageNum, 1);

    InsertResult<T> res = LeafNodeInsert(leaf, key, rowId);
    if(res.didSplit){
        if(leaf->header.isRoot) CreateNewRoot((NodeHeader*)leaf, res.splitKey, res.splitRowId, res.rightChildPageNum);
        else InsertIntoParent((NodeHeader*)leaf, res.splitKey, res.splitRowId, res.rightChildPageNum);
    } 
    return res;
}

template<typename T>
void Btree<T>::SelectRange(T L, T R, vector<uint32_t>& outRowIds){
    uint32_t leafPageNum = FindLeaf(rootPageNum, L, 0);

    bool firstPage = 1;
    
    while(leafPageNum != 0 || (firstPage && leafPageNum == 0)){
        LeafNode<T>* leaf = (LeafNode<T>*)pager->GetPage(leafPageNum, 1); // may change to not mark dirty later
        LeafNodeSelectRange(leaf, L, R, outRowIds);

        if(leaf->header.numCells > 0){
            T lastKey = leaf->cells[leaf->header.numCells - 1].key;
            if(lastKey > R) break;
        }
        firstPage = 0;
        leafPageNum = leaf->nextLeaf;
    }
}

template<typename T>
uint32_t Btree<T>::DeleteRange(T L, T R){
    uint32_t leafPageNum = FindLeaf(rootPageNum,L,0);
    uint32_t deletedCount = 0;
    bool firstPage = 1;
    
    while(leafPageNum != 0 || (firstPage && leafPageNum == 0)){
        LeafNode<T>* leaf = (LeafNode<T>*)pager->GetPage(leafPageNum, 1); // mat change to not mark dirty later
        deletedCount += LeafNodeDeleteRange(leaf, L, R);

        if(leaf->header.numCells > 0){
            T lastKey = leaf->cells[leaf->header.numCells - 1].key;
            if(lastKey > R) break;
        }
        firstPage = 0;
        leafPageNum = leaf->nextLeaf;
        //pager->Flush(leafPageNum, PAGE_SIZE);
    }

    return deletedCount;

}


template<typename T>
uint32_t Btree<T>::FindLeaf(uint32_t pageNum, T key, uint32_t rowId){
    void* node = pager->GetPage(pageNum, 0);
    NodeHeader* header = (NodeHeader*) node;

    if(header->type==LEAF) return pageNum;

    InternalNode<T>* internal = (InternalNode<T>*) node;
    uint32_t childPageNum = InternalNodeFindChild(internal, key, rowId);
    
    return FindLeaf(childPageNum, key, rowId); 
}




template<typename T>
void Btree<T>::InitializeLeafNode(LeafNode<T>* node){
    node->header.type = LEAF;
    node->header.isRoot = 0;
    node->header.numCells = 0;
    node->header.parent = 0;


    node->nextLeaf = 0;

    memset(node->cells, 0, LEAF_NODE_SIZE - HEADER_SIZE);
}

template<typename T>
bool Btree<T>::LeafNodeInsertNonFull(LeafNode<T>* node, T key, uint32_t rowId){
    uint16_t slot = LeafNodeFindSlot(node, key, rowId);
    

    if(node->header.numCells >= LEAF_NODE_MAX_CELLS) return 0;

    
    uint16_t cellsToMove = node->header.numCells - slot;
    if(cellsToMove > 0){
        void* src = &node->cells[slot];
        void* dest = &node->cells[slot+1];
        memmove(dest, src, cellsToMove*LEAF_CELL_SIZE);
    }
    node->header.numCells++;
    node->cells[slot].key = key;
    node->cells[slot].rowId = rowId;  

    return 1;
}


template<typename T>
InsertResult<T> Btree<T>::LeafNodeInsert(LeafNode<T>* node, T key, uint32_t rowId){
    T asdf; // a random T object to match InsertResult<T> attributes
    if(LeafNodeInsertNonFull(node, key, rowId)) return {true, false, asdf, 0, 0};


    uint32_t newPageNum = pager->numPages;
    
    LeafNode<T>* rightNode = (LeafNode<T>*) pager->GetPage(newPageNum, 1);
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

    T splitKey = rightNode->cells[0].key;
    uint32_t splitRowId = rightNode->cells[0].rowId;

    if(key>=splitKey) LeafNodeInsertNonFull(rightNode, key, rowId);
    else LeafNodeInsertNonFull(node, key, rowId);


    return {true, true, splitKey, splitRowId, newPageNum};

}

template<typename T>
uint16_t Btree<T>::LeafNodeFindSlot(LeafNode<T>* node, T targetKey, uint32_t targetRowId){
    uint16_t l = 0;
    uint16_t r = node->header.numCells;

    while(l<r){
        uint16_t mid = l+r>>1;
        T midKey = node->cells[mid].key;
        uint32_t midRowId = node->cells[mid].rowId;
        if(targetKey < midKey || (targetKey == midKey && targetRowId < midRowId)) r = mid;
        else l = mid+1;
    }

    return l;
}

template<typename T>
void Btree<T>::CreateNewRoot(NodeHeader* root, T splitKey, uint32_t splitRowId, uint32_t rightChildPageNum){
    pager->MarkDirty(rootPageNum);

    uint32_t leftChildPageNum = pager->numPages;
    NodeHeader* leftChild = (NodeHeader*)pager->GetPage(leftChildPageNum, 1);

    memcpy(leftChild, root, INTERNAL_NODE_SIZE);
    leftChild->isRoot = 0;
    leftChild->parent = 0;

    NodeHeader* rightChild = (NodeHeader*)pager->GetPage(rightChildPageNum, 1);
    rightChild->parent = 0;

    InternalNode<T>* internalRoot = (InternalNode<T>*)root;
    internalRoot->header.type = INTERNAL;
    internalRoot->header.isRoot = 1;
    internalRoot->header.numCells = 1;
    internalRoot->header.parent = 0;

    internalRoot->rightChild = rightChildPageNum;

    internalRoot->cells[0].key = splitKey;
    internalRoot->cells[0].rowId = splitRowId;
    internalRoot->cells[0].childPage = leftChildPageNum;

    if(leftChild->type == INTERNAL) UpdateChildParents((InternalNode<T>*)leftChild, leftChildPageNum);
    if(rightChild->type == INTERNAL) UpdateChildParents((InternalNode<T>*)rightChild, rightChildPageNum);
}

template<typename T>
uint32_t Btree<T>::InternalNodeFindChild(InternalNode<T>* node, T targetKey, uint32_t targetRowId){
    uint16_t l = 0;
    uint16_t r = node->header.numCells;

    while(l<r){
        uint16_t mid = l+r>>1;
        T midKey = node->cells[mid].key;
        uint32_t midRowId = node->cells[mid].rowId;
        if(targetKey < midKey || (targetKey == midKey && targetRowId < midRowId)) r=mid;
        else l = mid+1;
    }

    if(l == node->header.numCells) return node->rightChild;
    return node->cells[l].childPage;
}


template<typename T>
void Btree<T>::UpdateChildParents(InternalNode<T>* parentNode, uint32_t parentPageNum){
    void* child = pager->GetPage(parentNode->rightChild, 1);
    ((NodeHeader*)child)->parent = parentPageNum;

    for(uint16_t i = 0; i<parentNode->header.numCells;i++){
        void* child = pager->GetPage(parentNode->cells[i].childPage, 1);
        ((NodeHeader*)child)->parent = parentPageNum;
    }
}

template<typename T>
void Btree<T>::InsertIntoParent(NodeHeader* leftChild, T key, uint32_t rowId, uint32_t rightChildPageNum){
    uint32_t parentPageNum = leftChild->parent;

    if(parentPageNum == 0){
        InternalNode<T>* root = (InternalNode<T>*)pager->GetPage(0,1);

        InsertResult<T> res = InternalNodeInsert(root, key, rowId, rightChildPageNum);
        if(!res.didSplit) return;

        CreateNewRoot((NodeHeader*)root, res.splitKey, res.splitRowId, res.rightChildPageNum);
        
        return;
    }

    InternalNode<T>* parent = (InternalNode<T>*)pager->GetPage(parentPageNum, 1);
    InsertResult<T> res = InternalNodeInsert(parent, key, rowId, rightChildPageNum);

    if(res.didSplit){
        InsertIntoParent((NodeHeader*) parent, res.splitKey, res.splitRowId, res.rightChildPageNum);
    }
}

template<typename T>
InsertResult<T> Btree<T>:: InternalNodeInsert(InternalNode<T>* node, T key, uint32_t rowId, uint32_t rightChildPage){
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
        
        T asdf;
        return {true, false, asdf, 0, 0};
    }

    uint32_t newPageNum = pager->numPages;
    InternalNode<T>* rightNode = (InternalNode<T>*)pager->GetPage(newPageNum, 1);

    rightNode->header.type = INTERNAL;
    rightNode->header.isRoot = 0;
    rightNode->header.numCells = 0;
    rightNode->header.parent = node->header.parent;

    uint16_t splitIdx = INTERNAL_NODE_MAX_CELLS/2;

    T promotedKey = node->cells[splitIdx].key;
    uint32_t promotedRowId = node->cells[splitIdx].rowId;

    uint32_t leftNewRightChild = node->cells[splitIdx].childPage;

    uint16_t cellsMoved = node->header.numCells - splitIdx - 1;

    memcpy(rightNode->cells, &node->cells[splitIdx+1], cellsMoved*INTERNAL_CELL_SIZE);

    rightNode->header.numCells = cellsMoved;

    rightNode->rightChild = node->rightChild;
    node->rightChild = leftNewRightChild;
    node->header.numCells = splitIdx;


    if(key > promotedKey || (key==promotedKey && rowId > promotedRowId)){
        InternalNodeInsert(rightNode, key, rowId, rightChildPage);
    }
    else{
        InternalNodeInsert(node, key, rowId, rightChildPage);
    }

    UpdateChildParents(rightNode, newPageNum);

    return {true, true, promotedKey, promotedRowId, newPageNum};
}

template<typename T>
void Btree<T>::LeafNodeSelectRange(LeafNode<T>* node, T L, T R, vector<uint32_t>& outRowIds){
    uint16_t p = 0;
    for(uint16_t q = 0;q<node->header.numCells;q++){
        uint32_t rowId = node->cells[q].rowId;
        if(table->IsRowDeleted(rowId)) continue;

        T key = node->cells[q].key;
        if(L<=key && key<=R) outRowIds.push_back(rowId);

        if(p!=q) memcpy(&node->cells[p], &node->cells[q], LEAF_CELL_SIZE);
        p++;
    }

    node->header.numCells = p;

}

template<typename T>
uint16_t Btree<T>::LeafNodeDeleteRange(LeafNode<T>* node, T L, T R){
    uint16_t p = 0;
    uint16_t deletedCount = 0;
    for(uint16_t q = 0;q<node->header.numCells;q++){
        uint32_t rowId = node->cells[q].rowId;
        //if(table->IsRowDeleted(rowId)) continue;

        T key = node->cells[q].key;
        if(L<=key && key<=R){
            table->MarkRowDeleted(rowId);
            deletedCount++;
            continue;
        }

        if(p!=q) memcpy(&node->cells[p], &node->cells[q], LEAF_CELL_SIZE);
        p++;
    }
    
    node->header.numCells = p;

    return deletedCount;
}

