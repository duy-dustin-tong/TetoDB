// index_scan_executor.h

#pragma once

#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "index/abstract_index_iterator.h"
#include "index/b_plus_tree.h"
#include <memory>
#include <vector>


namespace tetodb {

class IndexScanExecutor : public AbstractExecutor {
public:
  IndexScanExecutor(ExecutionContext *exec_ctx, const IndexScanPlanNode *plan);

  void Init() override;
  bool Next(Tuple *tuple, RID *rid) override;
  const Schema *GetOutputSchema() override;

private:
  const IndexScanPlanNode *plan_;
  TableMetadata *table_metadata_;

  // Results from the B+ Tree Iterator
  std::unique_ptr<AbstractIndexIterator> iterator_;
  Tuple search_key_;
  const Schema *index_key_schema_;
};

} // namespace tetodb