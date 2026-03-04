-- Phase 2: After restart, INSERT into existing table
-- This exercises the lazy FSM: the FSM was NOT populated at startup,
-- so InsertTuple must trigger PopulateFSM() to find free space.
INSERT INTO fsm_test VALUES (4, 'delta_after_restart');
INSERT INTO fsm_test VALUES (5, 'epsilon_after_restart');
SELECT id, data FROM fsm_test;

-- Also test UPDATE after restart (also needs FSM for the new tuple)
UPDATE fsm_test SET data = 'modified_after_restart' WHERE id = 1;
SELECT id, data FROM fsm_test;

-- Verify DELETE works too
DELETE FROM fsm_test WHERE id = 3;
SELECT id, data FROM fsm_test;

DROP TABLE fsm_test;
