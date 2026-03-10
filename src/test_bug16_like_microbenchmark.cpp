#include <iostream>
#include <chrono>
#include <vector>
#include "type/value.h"

using namespace tetodb;
using namespace std::chrono;

int main() {
    std::cout << "--- TetoDB LIKE Operator Microbenchmark ---" << std::endl;
    
    // Test 1: Simple Literal Match
    Value v1(TypeId::VARCHAR, "TetoDB_Log_9999_Fast");
    Value p1(TypeId::VARCHAR, "TetoDB_Log_9999_Fast");
    
    // Test 2: Wildcard Suffix (Fast path matching)
    Value v2(TypeId::VARCHAR, "TetoDB_Log_9999_Fast");
    Value p2(TypeId::VARCHAR, "TetoDB_Log_%");
    
    // Test 3: Double Wildcard (Requires deep recursive backtracking)
    Value v3(TypeId::VARCHAR, "TetoDB_Log_9999_Fast_And_Furious_Data_Stream");
    Value p3(TypeId::VARCHAR, "%Fast%Data%");
    
    const int ITERATIONS = 1000000;
    
    // --- BENCHMARK 1 ---
    auto start1 = high_resolution_clock::now();
    for (int i = 0; i < ITERATIONS; i++) {
        volatile bool res = v1.CompareLike(p1); // volatile prevents compiler loop optimization
        (void)res;
    }
    auto end1 = high_resolution_clock::now();
    auto dura1 = duration_cast<microseconds>(end1 - start1).count();
    
    // --- BENCHMARK 2 ---
    auto start2 = high_resolution_clock::now();
    for (int i = 0; i < ITERATIONS; i++) {
        volatile bool res = v2.CompareLike(p2);
        (void)res;
    }
    auto end2 = high_resolution_clock::now();
    auto dura2 = duration_cast<microseconds>(end2 - start2).count();
    
    // --- BENCHMARK 3 ---
    auto start3 = high_resolution_clock::now();
    for (int i = 0; i < ITERATIONS; i++) {
        volatile bool res = v3.CompareLike(p3);
        (void)res;
    }
    auto end3 = high_resolution_clock::now();
    auto dura3 = duration_cast<microseconds>(end3 - start3).count();
    
    std::cout << "[Test 1 - Exact String] " << ITERATIONS << " ops took " << dura1 << " us (" << (dura1 / 1000.0) << " ms)\n";
    std::cout << "[Test 2 - Prefix Match]   " << ITERATIONS << " ops took " << dura2 << " us (" << (dura2 / 1000.0) << " ms)\n";
    std::cout << "[Test 3 - Backtracking] " << ITERATIONS << " ops took " << dura3 << " us (" << (dura3 / 1000.0) << " ms)\n";

    return 0;
}
