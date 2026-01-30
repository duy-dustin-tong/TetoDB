import subprocess
import os
import random
import string
import sys
import re
import argparse
import time

# --- CONFIGURATION ---
DB_EXE = "./TetoDB" if os.name != 'nt' else "TetoDB.exe"
DB_NAME_PREFIX = "acc_fuzz_db"
WAIT_TIME = 2 # Small wait to ensure OS flushes file buffers

def generate_random_string(length=16):
    return ''.join(random.choices(string.ascii_letters, k=length))

def clean_db_files():
    """Removes old DB files to ensure a fresh test."""
    extensions = [".db", ".teto", ".btree", ".tmp"]
    try:
        files = [f for f in os.listdir('.') if f.startswith(DB_NAME_PREFIX) and any(f.endswith(ext) for ext in extensions)]
        for f in files:
            os.remove(f)
    except Exception as e:
        pass

def generate_ground_truth(num_rows):
    print(f"   [1/5] Generating ground truth data ({num_rows} rows)...")
    data = {}
    ids = list(range(1, num_rows + 1))
    random.shuffle(ids)
    for uid in ids:
        data[uid] = generate_random_string()
    return data, ids

def generate_query_batch(all_ids, num_queries):
    print(f"   [2/5] Generating {num_queries} random queries...")
    batch = []
    
    for _ in range(num_queries):
        q_type = random.choice(["POINT", "RANGE", "RANGE"])
        
        if q_type == "POINT":
            target = random.choice(all_ids)
            batch.append({
                "type": "POINT",
                "start": target,
                "end": target
            })
        else:
            start = random.choice(all_ids)
            end = start + random.randint(0, 50) 
            batch.append({
                "type": "RANGE",
                "start": start,
                "end": end
            })
            
    return batch

def create_load_script(filename, table_name, use_index, data):
    print(f"   [3/5] Writing LOAD script {filename}...")
    with open(filename, "w") as f:
        idx_flag = "1" if use_index else "0"
        f.write(f"create table {table_name} id int {idx_flag} val char 32\n")
        
        # Bulk Insert
        for uid, val in data.items():
            f.write(f"insert into {table_name} {uid} \"{val}\"\n")
        
        f.write(".commit\n")
        f.write(".exit\n")

def create_query_script(filename, table_name, query_batch):
    print(f"   [4/5] Writing QUERY script {filename}...")
    with open(filename, "w") as f:
        # Note: No CREATE TABLE here. It must persist from Load phase.
        for q in query_batch:
            f.write(f"select from {table_name} where id {q['start']} {q['end']}\n")
        
        f.write(".exit\n")

def parse_db_output(raw_output):
    result_sets = []
    current_batch = {}
    
    # Matches "|  123  |  val  |"
    data_pattern = re.compile(r"^\|\s*(\d+)\s*\|\s*(\S+)\s*\|$")
    
    lines = raw_output.splitlines()
    for line in lines:
        line = line.strip()
        match = data_pattern.match(line)
        if match:
            if "id" in line.lower() and "val" in line.lower(): continue
            uid = int(match.group(1))
            val = match.group(2)
            current_batch[uid] = val
            
        if "rows in set" in line:
            result_sets.append(current_batch)
            current_batch = {}
            
    return result_sets

def verify_batch(data, query_batch, db_results):
    print(f"   [5/5] Verifying {len(query_batch)} queries...")
    
    if len(db_results) != len(query_batch):
        print(f"❌ CRITICAL FAIL: Query Count Mismatch!")
        print(f"   Expected: {len(query_batch)} results")
        print(f"   Received: {len(db_results)} results")
        print("   (This usually means the DB failed to persist the table or crashed)")
        return False

    errors = 0
    for i, (query, actual) in enumerate(zip(query_batch, db_results)):
        expected = {k: v for k, v in data.items() if query["start"] <= k <= query["end"]}
        
        keys_exp = set(expected.keys())
        keys_act = set(actual.keys())
        
        if keys_exp != keys_act:
            errors += 1
            if errors <= 3:
                print(f"   ❌ Query #{i+1} ({query['type']} {query['start']}-{query['end']}) FAILED")
                print(f"      Missing IDs: {sorted(list(keys_exp - keys_act))}")
                print(f"      Extra IDs:   {sorted(list(keys_act - keys_exp))}")
            continue

        for k in keys_exp:
            if expected[k] != actual[k]:
                errors += 1
                if errors <= 3:
                    print(f"   ❌ Query #{i+1} Value Mismatch for ID {k}")
                break

    if errors == 0:
        print(f"   ✅ All {len(query_batch)} queries matched.")
        return True
    else:
        print(f"   ❌ FAILED: {errors} queries had incorrect results.")
        return False

def run_test_mode(mode_name, use_index, num_rows, num_queries):
    print(f"\n=========================================")
    print(f"TEST RUN: {mode_name}")
    print(f"=========================================")
    
    clean_db_files()
    data, all_ids = generate_ground_truth(num_rows)
    batch = generate_query_batch(all_ids, num_queries)
    
    table_name = "fuzz_table"
    load_file = f"fuzz_{mode_name[:3]}_load.txt"
    query_file = f"fuzz_{mode_name[:3]}_query.txt"
    
    # 1. Generate Scripts
    create_load_script(load_file, table_name, use_index, data)
    create_query_script(query_file, table_name, batch)
    
    # 2. Run Load Phase
    print(f"   [EXEC] Loading Data...")
    try:
        subprocess.run([DB_EXE, DB_NAME_PREFIX, load_file], check=True, stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        print("   ❌ Load process crashed!")
        return False

    print(f"   [WAIT] Sleeping {WAIT_TIME}s for OS flush...")
    time.sleep(WAIT_TIME)

    # 3. Run Query Phase
    print(f"   [EXEC] Running Queries...")
    try:
        process = subprocess.run(
            [DB_EXE, DB_NAME_PREFIX, query_file],
            capture_output=True,
            text=True
        )
        parsed_results = parse_db_output(process.stdout)
        return verify_batch(data, batch, parsed_results)
    except Exception as e:
        print(f"   ❌ Query Execution Error: {e}")
        return False

def main():
    if not os.path.exists(DB_EXE):
        print(f"Error: {DB_EXE} not found.")
        return

    parser = argparse.ArgumentParser()
    parser.add_argument("rows", nargs="?", type=int, default=1000, help="Number of rows")
    parser.add_argument("--queries", type=int, default=50, help="Number of queries")
    args = parser.parse_args()
    
    print(f"FUZZ TESTER (Split Persistence): {args.rows} Rows")
    
    pass_no = run_test_mode("No_Index", False, args.rows, args.queries)
    pass_idx = run_test_mode("Index", True, args.rows, args.queries)
    
    print("\n" + "="*40)
    print("FINAL SUMMARY")
    print("="*40)
    print(f"No Index:   {'✅ PASS' if pass_no else '❌ FAIL'}")
    print(f"With Index: {'✅ PASS' if pass_idx else '❌ FAIL'}")

if __name__ == "__main__":
    main()