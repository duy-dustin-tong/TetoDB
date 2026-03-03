// abstract_expression.h

#pragma once

#include <vector>
#include <memory>
#include "storage/table/tuple.h"
#include "type/value.h"

namespace tetodb {

    class AbstractExpression {
    public:
        // Expressions form a tree, so they need to own their children.
        AbstractExpression(std::vector<std::unique_ptr<AbstractExpression>> children)
            : children_(std::move(children)) {
        }

        virtual ~AbstractExpression() = default;

        virtual Value Evaluate(const Tuple* tuple, const Schema* schema, const std::vector<Value>* params = nullptr) const = 0;

        virtual Value EvaluateJoin(const Tuple* left_tuple, const Schema* left_schema,
            const Tuple* right_tuple, const Schema* right_schema, const std::vector<Value>* params = nullptr) const = 0;

        // AST Traversal Helpers
        const AbstractExpression* GetChildAt(uint32_t child_idx) const {
            return children_[child_idx].get();
        }

        const std::vector<std::unique_ptr<AbstractExpression>>& GetChildren() const {
            return children_;
        }

    private:
        std::vector<std::unique_ptr<AbstractExpression>> children_;
    };

} // namespace tetodb