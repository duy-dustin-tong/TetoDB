#include "type/value.h"
#include <iostream>
#include <vector>


using namespace tetodb;

int main() {
  std::cout << "--- TetoDB Bug 17 Division Benchmark ---" << std::endl;

  try {
        Value a(TypeId::INTEGER, 10);
        Value b(TypeId::DECIMAL, 0.4);
        
        std::cout << "Attempting to divide 10 by 0.4..." << std::endl;
        Value result = a.Divide(b);
        std::cout << "Result: " << result.ToString() << std::endl;
        
    } catch (const std::exception& e) {
        std::cout << "Caught standard exception: " << e.what() << std::endl;
    } catch (...) {
        std::cout << "Caught unknown exception!" << std::endl;
    }
    
    try {
        Value c(TypeId::INTEGER, 10);
        Value d(TypeId::DECIMAL, 0.0);
        
        std::cout << "Attempting to divide 10 by 0.0..." << std::endl;
        Value result = c.Divide(d);
        std::cout << "Result: " << result.ToString() << std::endl;
        
    } catch (const std::exception& e) {
        std::cout << "Caught standard exception: " << e.what() << std::endl;
  } catch (...) {
    std::cout << "Caught unknown exception!" << std::endl;
  }

  std::cout << "Successfully survived all divisions." << std::endl;
  return 0;
}
