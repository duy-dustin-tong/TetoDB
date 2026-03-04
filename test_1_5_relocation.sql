-- ============================================
-- TEST 1.5: Update Executor - Large tuple relocation
-- ============================================
-- This tests that when an UPDATE makes a tuple MUCH larger,
-- forcing it to relocate to a different page, the data is
-- still correctly retrievable and indexes remain consistent.

CREATE TABLE reloc_test (id INTEGER PRIMARY KEY, data VARCHAR);

-- Insert small tuples first
INSERT INTO reloc_test VALUES (1, 'tiny');
INSERT INTO reloc_test VALUES (2, 'small');
INSERT INTO reloc_test VALUES (3, 'mini');

-- Verify initial state
SELECT id, data FROM reloc_test;

-- Update to a MUCH larger value - this should force page relocation
UPDATE reloc_test SET data = 'THIS_IS_A_VERY_LONG_STRING_THAT_IS_DESIGNED_TO_BE_MUCH_LARGER_THAN_THE_ORIGINAL_VALUE_SO_THAT_IT_FORCES_THE_TUPLE_TO_BE_RELOCATED_TO_A_DIFFERENT_PAGE_IN_THE_TABLE_HEAP_WHICH_EXERCISES_THE_STALE_RID_FIX_WE_MADE_IN_THE_UPDATE_EXECUTOR' WHERE id = 1;

-- Can we still find it?
SELECT id, data FROM reloc_test WHERE id = 1;

-- Can we still find all rows?
SELECT id, data FROM reloc_test;

-- Update another row with large data
UPDATE reloc_test SET data = 'ANOTHER_LARGE_STRING_THAT_SHOULD_ALSO_RELOCATE_THE_TUPLE_AND_VERIFY_THAT_INDEX_ENTRIES_POINT_TO_THE_CORRECT_NEW_RID_AFTER_RELOCATION_HAPPENS_DURING_THE_UPDATE_PROCESS' WHERE id = 2;

SELECT id, data FROM reloc_test;

-- Verify PK index still works - update by PK after relocation
UPDATE reloc_test SET data = 'final' WHERE id = 1;
SELECT id, data FROM reloc_test WHERE id = 1;

DROP TABLE reloc_test;
