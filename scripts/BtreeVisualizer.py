import sys
import struct

# CONFIGURATION (Must match your C++ Btree.h)
PAGE_SIZE = 4096
# Header: Type(1), IsRoot(1), NumCells(2), Parent(4)
HEADER_FMT = "BBHi" 
HEADER_SIZE = 8

# Leaf Body: NextLeaf(4) + Cells...
LEAF_NEXT_POINTER_SIZE = 4

# Cell Sizes
LEAF_CELL_FMT = "ii"  # int32 key, int32 rowId
LEAF_CELL_SIZE = 8

INTERNAL_CELL_FMT = "iii" # int32 key, int32 rowId, uint32 childPage
INTERNAL_CELL_SIZE = 12

NODE_INTERNAL = 0
NODE_LEAF = 1

def read_page(f, page_num):
    f.seek(page_num * PAGE_SIZE)
    data = f.read(PAGE_SIZE)
    if not data: return None
    
    header_raw = data[:HEADER_SIZE]
    node_type, is_root, num_cells, parent = struct.unpack(HEADER_FMT, header_raw)
    
    return {
        "page": page_num,
        "type": "LEAF" if node_type == NODE_LEAF else "INTERNAL",
        "is_root": is_root,
        "num_cells": num_cells,
        "parent": parent,
        "data": data
    }

def verify_tree(filename):
    with open(filename, "rb") as f:
        root = read_page(f, 0)
        if not root:
            print("Empty file")
            return

        print(f"ROOT: {root['page']} (Type: {root['type']})")
        
        queue = [0]
        visited = set()
        
        # Track lineage to verify parents
        child_to_parent_map = {}
        
        # Track all leaves to verify linked list later
        leaf_pages = []

        while queue:
            curr_page_num = queue.pop(0)
            if curr_page_num in visited:
                print(f"ERROR: Cycle detected at Page {curr_page_num}")
                continue
            visited.add(curr_page_num)

            node = read_page(f, curr_page_num)
            
            # 1. Print Node Info
            print(f"\n[Page {curr_page_num}] Type: {node['type']}, Parent: {node['parent']}, Cells: {node['num_cells']}")

            # 2. Verify Parent Pointer
            if curr_page_num != 0:
                expected_parent = child_to_parent_map.get(curr_page_num, -1)
                if node['parent'] != expected_parent:
                    print(f"  >>> CRITICAL ERROR: Page {curr_page_num} thinks parent is {node['parent']}, but it is actually child of {expected_parent}")

            # 3. Parse Body based on Type
            if node['type'] == "INTERNAL":
                # Internal Node Layout: [Header] [RightChildPtr] [Cell 0] [Cell 1]...
                
                # Right Child Pointer is immediately after header (Offset 8)
                right_child = struct.unpack("I", node['data'][HEADER_SIZE : HEADER_SIZE+4])[0]
                
                print(f"  -> Right Child: {right_child}")
                
                # Double Parent Check
                if right_child in child_to_parent_map:
                     print(f"  >>> CRITICAL ERROR: Page {right_child} is owned by multiple parents! (Double Parent Bug)")
                
                child_to_parent_map[right_child] = curr_page_num
                queue.append(right_child)

                # Cells start after Header + RightChildPtr = 12
                cell_offset = HEADER_SIZE + 4
                for i in range(node['num_cells']):
                    raw_cell = node['data'][cell_offset : cell_offset + INTERNAL_CELL_SIZE]
                    key, row_id, child_page = struct.unpack(INTERNAL_CELL_FMT, raw_cell)
                    
                    print(f"  -> Key: {key}, Child: {child_page}")
                    
                    if child_page in child_to_parent_map:
                        print(f"  >>> CRITICAL ERROR: Page {child_page} is owned by multiple parents! (Double Parent Bug)")
                    
                    child_to_parent_map[child_page] = curr_page_num
                    queue.append(child_page)
                    cell_offset += INTERNAL_CELL_SIZE

            elif node['type'] == "LEAF":
                leaf_pages.append(curr_page_num)
                
                # Leaf Node Layout: [Header] [NextLeafPtr] [Cell 0] [Cell 1]...
                
                # Next Leaf Pointer is immediately after header (Offset 8)
                next_leaf = struct.unpack("I", node['data'][HEADER_SIZE : HEADER_SIZE+4])[0]
                print(f"  -> Next Leaf: {next_leaf}")

                # Cells start after Header + NextLeafPtr = 12
                offset = HEADER_SIZE + LEAF_NEXT_POINTER_SIZE
                keys = []
                for i in range(node['num_cells']):
                    raw_cell = node['data'][offset : offset + LEAF_CELL_SIZE]
                    key, row_id = struct.unpack(LEAF_CELL_FMT, raw_cell)
                    keys.append(key)
                    offset += LEAF_CELL_SIZE
                print(f"  Keys: {keys}")

        # 4. Verify Linked List (Scan Logic)
        print("\n--- Verifying Linked List Structure ---")
        if not leaf_pages: return

        # Find the left-most leaf (It's the one that is never a 'next_leaf' of anyone else)
        # Actually, standard traversal finds leaves in order left-to-right.
        # But our BFS 'queue' puts them in 'depth' order, not 'left-to-right' order.
        # We need to traverse from the root to find the Left-Most Leaf.
        
        curr = 0
        while True:
            n = read_page(f, curr)
            if n['type'] == "LEAF":
                left_most_leaf = curr
                break
            # Go to the first child (Cell 0's child)
            # Internal Layout: Header(8) + RightChild(4) + Cell0(Key, Row, Child)
            # Cell 0 Child is at offset 12 + 8 = 20 bytes? No.
            # Internal Cell: Key(4), Row(4), Child(4). Child is at end of cell.
            # So Header(8) + RightChild(4) + Cell0[Key(4), Row(4), Child(4)]
            # Child is at 8+4+4+4 = 20? No.
            # Offset = 12. Cell0 starts. Child is at 12 + 8 = 20.
            if n['num_cells'] > 0:
                first_cell_child = struct.unpack("I", n['data'][20:24])[0]
                curr = first_cell_child
            else:
                # If internal node has no cells, follow right child
                right_child = struct.unpack("I", n['data'][8:12])[0]
                curr = right_child

        print(f"Left-Most Leaf is Page {left_most_leaf}. Walking the chain...")
        
        visited_leaves = 0
        curr = left_most_leaf
        last_key = -1
        
        while curr != 0:
            visited_leaves += 1
            n = read_page(f, curr)
            
            # Verify Sorting across pages
            offset = HEADER_SIZE + LEAF_NEXT_POINTER_SIZE
            for i in range(n['num_cells']):
                raw_cell = n['data'][offset : offset + LEAF_CELL_SIZE]
                key, _ = struct.unpack(LEAF_CELL_FMT, raw_cell)
                if key < last_key:
                    print(f"  >>> CRITICAL ERROR: Sort Order Violated! Page {curr} has key {key} which is < previous key {last_key}")
                last_key = key
                offset += LEAF_CELL_SIZE
                
            next_leaf = struct.unpack("I", n['data'][HEADER_SIZE : HEADER_SIZE+4])[0]
            print(f"  Page {curr} -> Page {next_leaf}")
            curr = next_leaf
            
            if visited_leaves > 1000:
                print("  >>> Error: Infinite Loop in Linked List!")
                break

        print(f"Walked {visited_leaves} leaf pages.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_tree.py <filename>")
    else:
        verify_tree(sys.argv[1])