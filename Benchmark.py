import subprocess
import random
import string
import os
import time
import re
import sys
import argparse

# --- CONFIGURATION ---
DEFAULT_ROWS = 50000
DB_EXE = "./TetoDB" if os.name != 'nt' else "TetoDB.exe"
DB_NAME_PREFIX = "bench_db"
WAIT_TIME = 5 # Seconds to wait for I/O cooldown

# Operation Counts (Higher = More Statistically Significant)
NUM_POINT_SELECTS = 1000
NUM_RANGE_SELECTS = 100
NUM_POINT_DELETES = 1000
NUM_RANGE_DELETES = 100

def generate_random_string(length=16):
    letters = string.ascii_letters
    return '"' + ''.join(random.choice(letters) for i in range(length)) + '"'

def generate_dataset(num_rows):
    print(f"Pre-generating data for {num_rows} rows in memory...")
    data = []
    ids = list(range(1, num_rows + 1))
    random.shuffle(ids)
    for uid in ids:
        data.append((uid, generate_random_string()))
    return data, ids

def generate_query_set(all_ids, num_rows):
    """
    Generates a fixed list of operations with SCALED range sizes.
    Range Size = ~1% of Total Rows.
    """
    
    # Scale range size: 1% of DB size (min 10 rows)
    range_size = max(10, int(num_rows * 0.01))
    
    print(f"Generating consistent query set (Range Size: {range_size} rows)...")
    ops = []

    # 1. Point Selects
    for _ in range(NUM_POINT_SELECTS):
        ops.append(("POINT_SEL", random.choice(all_ids)))

    # 2. Range Selects
    for _ in range(NUM_RANGE_SELECTS):
        start = random.choice(all_ids)
        ops.append(("RANGE_SEL", start, start + range_size))

    # 3. Point Deletes
    for _ in range(NUM_POINT_DELETES):
        ops.append(("POINT_DEL", random.choice(all_ids)))

    # 4. Range Deletes (Half the size of Selects to be safe)
    for _ in range(NUM_RANGE_DELETES):
        start = random.choice(all_ids)
        ops.append(("RANGE_DEL", start, start + (range_size // 2)))
        
    return ops, range_size

def create_load_script(filename, table_name, use_index, dataset):
    print(f"Writing LOAD script {filename}...")
    with open(filename, "w") as f:
        idx_flag = "1" if use_index else "0"
        f.write(f"create table {table_name} id int {idx_flag} val char 32\n")
        
        num_rows = len(dataset)
        commit_step = max(5000, num_rows // 20)
        progress_step = max(1, num_rows // 20)

        for i, (uid, val) in enumerate(dataset):
            f.write(f"insert into {table_name} {uid} {val}\n")
            if (i + 1) % progress_step == 0:
                percent = ((i + 1) / num_rows) * 100
                sys.stdout.write(f"\r   [Generating Load: {int(percent)}%] {i+1}/{num_rows}")
                sys.stdout.flush()
            if (i + 1) % commit_step == 0:
                f.write(".commit\n")
        
        f.write("\n.commit\n")
        f.write(".exit\n")
    print("")

def create_query_script(filename, table_name, query_set):
    """Writes the query script using the pre-generated operation list."""
    print(f"Writing QUERY script {filename}...")
    with open(filename, "w") as f:
        for op in query_set:
            if op[0] == "POINT_SEL":
                f.write(f"select from {table_name} where id {op[1]} {op[1]}\n")
            elif op[0] == "RANGE_SEL":
                f.write(f"select from {table_name} where id {op[1]} {op[2]}\n")
            elif op[0] == "POINT_DEL":
                f.write(f"delete from {table_name} where id {op[1]} {op[1]}\n")
            elif op[0] == "RANGE_DEL":
                f.write(f"delete from {table_name} where id {op[1]} {op[2]}\n")
        
        f.write(".commit\n")
        f.write(".exit\n")

def run_process(script_name, total_rows_for_progress=0, is_loading=False):
    print(f"   -> Executing {script_name}...")
    
    process = subprocess.Popen(
        [DB_EXE, DB_NAME_PREFIX, script_name], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1 
    )

    metrics = {
        "insert": [], "pt_sel": [], "rg_sel": [], "pt_del": [], "rg_del": []
    }
    
    last_cmd = "UNKNOWN"
    sel_count = 0
    del_count = 0
    inserts_done = 0
    update_interval = max(1, total_rows_for_progress // 20) if total_rows_for_progress > 0 else 1
    start_time = time.time()

    for line in process.stdout:
        if "row inserted" in line:
            last_cmd = "INSERT"
            if is_loading:
                inserts_done += 1
                if inserts_done % update_interval == 0:
                    elapsed = time.time() - start_time
                    percent = (inserts_done / total_rows_for_progress) * 100
                    if elapsed > 0:
                        eta = (total_rows_for_progress - inserts_done) / (inserts_done / elapsed)
                        print(f"      [Loading: {int(percent)}%] ETA: {eta:.1f}s")

        elif "rows in set" in line:
            last_cmd = "SELECT"
            sel_count += 1
            
        elif "Deleted" in line:
            last_cmd = "DELETE"
            del_count += 1

        match = re.search(r"\((\d+\.\d+) (ms|us)\)", line)
        if match:
            val = float(match.group(1))
            unit = match.group(2)
            if unit == "us": val /= 1000.0
            
            if last_cmd == "INSERT":
                metrics["insert"].append(val)
            elif last_cmd == "SELECT":
                if sel_count <= NUM_POINT_SELECTS:
                    metrics["pt_sel"].append(val)
                else:
                    metrics["rg_sel"].append(val)
            elif last_cmd == "DELETE":
                if del_count <= NUM_POINT_DELETES:
                    metrics["pt_del"].append(val)
                else:
                    metrics["rg_del"].append(val)
            
            last_cmd = "UNKNOWN"

    process.wait()
    
    avgs = {}
    for key, lst in metrics.items():
        if lst: avgs[key] = sum(lst) / len(lst)
        else: avgs[key] = 0.0
            
    return avgs

def run_test_suite(table_name, use_index, dataset, query_set, num_rows):
    # Cleanup
    for ext in [".db", ".teto", ".btree", ".tmp"]:
        try:
            files = [f for f in os.listdir('.') if f.startswith(DB_NAME_PREFIX) and f.endswith(ext)]
            for f in files: os.remove(f)
        except: pass

    suffix = "with_index" if use_index else "no_index"
    load_file = f"bench_{suffix}_load.txt"
    query_file = f"bench_{suffix}_query.txt"

    create_load_script(load_file, table_name, use_index, dataset)
    create_query_script(query_file, table_name, query_set)

    print(f"\n[PHASE 1] Loading Data ({table_name})...")
    m_load = run_process(load_file, num_rows, is_loading=True)
    
    print(f"\n[WAIT] Sleeping {WAIT_TIME}s for I/O cooldown...")
    time.sleep(WAIT_TIME)

    print(f"\n[PHASE 2] Running Queries ({table_name})...")
    m_query = run_process(query_file, 0, is_loading=False)

    return {
        "insert": m_load["insert"],
        "pt_sel": m_query["pt_sel"],
        "rg_sel": m_query["rg_sel"],
        "pt_del": m_query["pt_del"],
        "rg_del": m_query["rg_del"]
    }

def main():
    if not os.path.exists(DB_EXE):
        print(f"Error: {DB_EXE} not found.")
        return

    parser = argparse.ArgumentParser()
    parser.add_argument("rows", nargs="?", type=int, default=DEFAULT_ROWS)
    args = parser.parse_args()
    num_rows = args.rows

    dataset, all_ids = generate_dataset(num_rows)
    # Pass num_rows to scale the query range size
    query_set, range_sz = generate_query_set(all_ids, num_rows)

    print("=" * 60)
    print(f"BENCHMARK: {num_rows} ROWS")
    print(f" - Range Size: {range_sz} Rows (1% of Total)")
    print(f" - {NUM_POINT_SELECTS} Point Selects")
    print(f" - {NUM_RANGE_SELECTS} Range Selects")
    print(f" - {NUM_POINT_DELETES} Point Deletes")
    print(f" - {NUM_RANGE_DELETES} Range Deletes")
    print("=" * 60)

    print(f"\n[SETUP WAIT] Sleeping {WAIT_TIME}s to stabilize system...")
    time.sleep(WAIT_TIME)

    print(">>> BENCHMARK 1: NO INDEX (Linear Scan)")
    res_no = run_test_suite("table_slow", False, dataset, query_set, num_rows)
    
    print(f"\n[INTERMISSION] Sleeping {WAIT_TIME}s...")
    time.sleep(WAIT_TIME)

    print("\n>>> BENCHMARK 2: WITH INDEX (B-Tree)")
    res_idx = run_test_suite("table_fast", True, dataset, query_set, num_rows)
    
    print("\n" + "=" * 100)
    print(f"STATISTICAL RESULTS ({num_rows} Rows)")
    print("=" * 100)

    def get_speedup(slow, fast):
        if fast == 0: return "Inf"
        return f"{slow/fast:.1f}x"

    print(f"{'Metric (Average Time)':<30} | {'No Index':<15} | {'B-Tree':<15} | {'Speedup'}")
    print("-" * 100)
    
    print(f"{'Insert':<30} | {res_no['insert']:.4f} ms      | {res_idx['insert']:.4f} ms      | Overhead: {res_idx['insert'] - res_no['insert']:.4f} ms")
    print(f"{'Point SELECT':<30} | {res_no['pt_sel']:.4f} ms      | {res_idx['pt_sel']:.4f} ms      | {get_speedup(res_no['pt_sel'], res_idx['pt_sel'])}")
    print(f"{'Range SELECT (' + str(range_sz) + ' rows)':<30} | {res_no['rg_sel']:.4f} ms      | {res_idx['rg_sel']:.4f} ms      | {get_speedup(res_no['rg_sel'], res_idx['rg_sel'])}")
    print(f"{'Point DELETE':<30} | {res_no['pt_del']:.4f} ms      | {res_idx['pt_del']:.4f} ms      | {get_speedup(res_no['pt_del'], res_idx['pt_del'])}")
    print(f"{'Range DELETE':<30} | {res_no['rg_del']:.4f} ms      | {res_idx['rg_del']:.4f} ms      | {get_speedup(res_no['rg_del'], res_idx['rg_del'])}")
    print("-" * 100)

if __name__ == "__main__":
    main()