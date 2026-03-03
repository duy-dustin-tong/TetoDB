#pragma once

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace tetodb {

    enum class ExceptionType {
        INVALID = 0,        // Invalid type
        OUT_OF_RANGE = 1,   // Index out of range
        CONVERSION = 2,     // Casting error
        UNKNOWN_TYPE = 3,   // Unknown type
        DECIMAL = 4,        // Decimal related
        MISMATCH_TYPE = 5,  // Type mismatch
        DIVIDE_BY_ZERO = 6, // Divide by zero
        OBJECT_SIZE = 7,    // Object size mismatch
        INCOMPATIBLE_TYPE = 8, // Incompatible type
        OUT_OF_MEMORY = 9,  // Out of memory
        NOT_IMPLEMENTED = 10 // Method not implemented
    };

    class Exception : public std::runtime_error {
    public:
        explicit Exception(const std::string& message) : std::runtime_error(message), type_(ExceptionType::INVALID) {
            std::string exception_message = "Message :: " + message + "\n";
            std::cerr << exception_message;
        }

        Exception(ExceptionType exception_type, const std::string& message)
            : std::runtime_error(message), type_(exception_type) {
            std::string exception_type_type;
            switch (exception_type) {
            case ExceptionType::INVALID:
                exception_type_type = "INVALID";
                break;
            case ExceptionType::OUT_OF_RANGE:
                exception_type_type = "OUT_OF_RANGE";
                break;
            case ExceptionType::CONVERSION:
                exception_type_type = "CONVERSION";
                break;
            case ExceptionType::UNKNOWN_TYPE:
                exception_type_type = "UNKNOWN_TYPE";
                break;
            case ExceptionType::DECIMAL:
                exception_type_type = "DECIMAL";
                break;
            case ExceptionType::MISMATCH_TYPE:
                exception_type_type = "MISMATCH_TYPE";
                break;
            case ExceptionType::DIVIDE_BY_ZERO:
                exception_type_type = "DIVIDE_BY_ZERO";
                break;
            case ExceptionType::OBJECT_SIZE:
                exception_type_type = "OBJECT_SIZE";
                break;
            case ExceptionType::INCOMPATIBLE_TYPE:
                exception_type_type = "INCOMPATIBLE_TYPE";
                break;
            case ExceptionType::OUT_OF_MEMORY:
                exception_type_type = "OUT_OF_MEMORY";
                break;
            case ExceptionType::NOT_IMPLEMENTED:
                exception_type_type = "NOT_IMPLEMENTED";
                break;
            default:
                exception_type_type = "UNKNOWN";
            }
            std::string exception_message =
                "\nException Type :: " + exception_type_type + "\nMessage :: " + message + "\n";
            std::cerr << exception_message;
        }

        ExceptionType GetType() const { return type_; }

    private:
        ExceptionType type_;
    };

} // namespace tetodb