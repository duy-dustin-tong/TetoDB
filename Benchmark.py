import subprocess
import random
import string
import os
import time
import re
import sys
import argparse

# --- DEFAULTS ---
DEFAULT_ROWS = 50000
DB_EXE = "./TetoDB" if os.name != 'nt' else "TetoDB.exe"
DB_NAME = "bench_db"

def generate_random_string(length=16):
    letters = string.ascii_letters
    return '"' + ''.join(random.choice(letters) for i in range(length)) + '"'

def create_script(filename, table_name, use_index, num_rows, target_id):
    print(f"Generating {filename} with {num_rows} rows...")
    print(f"  -> Indexing: {'ON' if use_index else 'OFF'}")
    
    with open(filename, "w") as f:
        # 1. Create Table
        idx_flag = "1" if use_index else "0"
        f.write(f"create table {table_name} id int {idx_flag} val char 32\n")
        
        # 2. Generate IDs
        ids = list(range(1, num_rows + 1))
        random.shuffle(ids)
        
        # 3. Write Inserts
        progress_step = max(10000, num_rows // 10)
        commit_step = max(5000, num_rows // 20)

        for i, user_id in enumerate(ids):
            f.write(f"insert into {table_name} {user_id} {generate_random_string()}\n")
            if (i + 1) % progress_step == 0:
                print(f"     ... {i + 1} rows written")
            if (i + 1) % commit_step == 0:
                f.write(".commit\n")
        
        # 4. Final Commit & Test Query
        f.write(".commit\n")
        f.write(f"select from {table_name} where id {target_id} {target_id}\n")
        f.write(".exit\n")

def run_test(script_name, target_id):
    # Cleanup old DB files
    for ext in [".db", ".teto", ".btree"]:
        try:
            files = [f for f in os.listdir('.') if f.endswith(ext) and DB_NAME in f]
            for f in files: os.remove(f)
        except: pass

    print(f"Running TetoDB with {script_name}...")
    
    start_time = time.time()
    
    process = subprocess.Popen(
        [DB_EXE, DB_NAME, script_name], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1 
    )

    last_select_time = 0.0
    total_insert_time = 0.0
    insert_count = 0
    
    print("\n--- DATABASE OUTPUT (VERIFICATION) ---")
    
    for line in process.stdout:
        # 1. PRINT THE ACTUAL RESULT TABLE
        # We print lines that look like table borders (+) or data rows (|)
        if line.startswith("+") or line.startswith("|"):
            print("    " + line.strip()) # Indent it slightly

        # 2. Capture Timing
        match = re.search(r"\((\d+\.\d+) (ms|us)\)", line)
        if match:
            val = float(match.group(1))
            unit = match.group(2)
            if unit == "us": val /= 1000.0 # Convert microseconds to ms
            
            last_select_time = val
            total_insert_time += val
            insert_count += 1
            
    print("--------------------------------------\n")
            
    process.wait()
    total_wall_time = time.time() - start_time
    
    if insert_count > 1:
        avg_insert = (total_insert_time - last_select_time) / (insert_count - 1)
    else:
        avg_insert = 0
        
    return avg_insert, last_select_time, total_wall_time

def main():
    if not os.path.exists(DB_EXE):
        print(f"Error: {DB_EXE} not found.")
        return

    parser = argparse.ArgumentParser()
    parser.add_argument("rows", nargs="?", type=int, default=DEFAULT_ROWS)
    args = parser.parse_args()

    num_rows = args.rows
    target_id = int(num_rows * 0.8) 

    print("=" * 60)
    print(f"BENCHMARK: {num_rows} ROWS | SEARCHING FOR ID: {target_id}")
    print("=" * 60)

    create_script("bench_no_index.txt", "table_slow", False, num_rows, target_id)
    create_script("bench_with_index.txt", "table_fast", True, num_rows, target_id)
    
    print("=" * 60)
    
    print(">>> BENCHMARK 1: NO INDEX (Linear Scan)")
    avg_ins_no, sel_no, wall_no = run_test("bench_no_index.txt", target_id)
    print(f"    SELECT Time: {sel_no:.4f} ms")
    print("-" * 60)

    print(">>> BENCHMARK 2: WITH INDEX (B-Tree)")
    avg_ins_idx, sel_idx, wall_idx = run_test("bench_with_index.txt", target_id)
    print(f"    SELECT Time: {sel_idx:.4f} ms")
    
    print("=" * 60)
    
    # Calculate speedup
    if sel_idx > 0:
        speedup = f"{sel_no / sel_idx:.1f}x"
    else:
        speedup = "Infinite (Too fast to measure)"

    print(f"SPEEDUP FACTOR: {speedup}")

if __name__ == "__main__":
    main()