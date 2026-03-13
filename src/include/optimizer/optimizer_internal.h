// optimizer_internal.h

// What it is: Definitions of helper functions that apply specific logic. Separating this keeps your main header clean.

// Examples:
//     OptimizeMergeFilterScan: Merges "Filter (x > 5)" and "SeqScan" into a single optimized step.
//     OptimizeMergeProjection: Removes unnecessary column copying.
//     OptimizeNLJoin: Decides order of tables in a Join.