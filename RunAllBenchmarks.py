import subprocess
import os
import sys

# --- CONFIGURATION ---
BENCHMARK_SCRIPT = "Benchmark.py"
OUTPUT_FILE = "benchmark_report.txt"

# Format: (Rows, Repetitions)
RUN_CONFIG = [
    (100, 2),
    (10000, 2),
    (1000000, 2),
    (10000000, 2),
    (100000000, 1) # Warning: This will take a significant amount of time
]

def run_benchmark(num_rows, run_index):
    """Executes the benchmark script and returns the stdout."""
    cmd = [sys.executable, BENCHMARK_SCRIPT, str(num_rows)]
    print(f"\n>>> EXECUTING RUN {run_index} FOR {num_rows} ROWS...")
    
    try:
        # Run and capture output text
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running benchmark: {e}")
        return None

def extract_results(output):
    """Extracts only the Final Statistical Results table from the output."""
    lines = output.splitlines()
    extracted_lines = []
    capture = False
    separator_count = 0 
    
    for line in lines:
        # 1. Start capturing when we see the banner
        if "STATISTICAL RESULTS" in line:
            capture = True
            extracted_lines.append("=" * 100) 
            extracted_lines.append(line)
            continue
        
        if capture:
            extracted_lines.append(line)
            
            # 2. Count separator lines (starts with -----)
            if line.startswith("-----") and len(line) > 50:
                separator_count += 1
                
                # 3. Stop ONLY after we've seen the SECOND separator (bottom of table)
                if separator_count >= 2:
                    break
    
    if not extracted_lines:
        return "Error: Could not find results table in output."
    
    return "\n".join(extracted_lines)

def main():
    # check if benchmark script exists
    if not os.path.exists(BENCHMARK_SCRIPT):
        print(f"Error: {BENCHMARK_SCRIPT} not found in current directory.")
        return

    # Clear previous report
    with open(OUTPUT_FILE, "w") as f:
        f.write("TetoDB BENCHMARK SUITE REPORT\n")
        f.write("=============================\n\n")

    total_runs = sum(r[1] for r in RUN_CONFIG)
    current_run = 0

    for size, repetitions in RUN_CONFIG:
        for i in range(repetitions):
            current_run += 1
            print(f"--- Progress: {current_run}/{total_runs} (Size: {size}) ---")
            
            # 1. Run the Benchmark
            full_output = run_benchmark(size, i+1)
            
            if full_output:
                # 2. Extract Table
                table_text = extract_results(full_output)
                
                # 3. Print to console (optional confirmation)
                print("Captured Result:")
                print(table_text)
                
                # 4. Save to file
                with open(OUTPUT_FILE, "a") as f:
                    f.write(f"RUN #{i+1} - SIZE: {size}\n")
                    f.write(table_text)
                    f.write("\n\n")
            else:
                print("Skipping result save due to error.")

    print(f"\n✅ All benchmarks completed. Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()