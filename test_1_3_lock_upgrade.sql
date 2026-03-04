-- ============================================
-- TEST 1.3: Lock Upgrade (S -> X) via UPDATE
-- ============================================
-- This tests that a transaction holding a Shared lock
-- can upgrade to Exclusive when UPDATE needs write access.
-- The UPDATE executor reads first (S lock), then writes (needs X).
-- Before the fix, LockExclusive returned false when holding S.

CREATE TABLE lock_upgrade_test (id INTEGER PRIMARY KEY, val VARCHAR);
INSERT INTO lock_upgrade_test VALUES (1, 'original');
INSERT INTO lock_upgrade_test VALUES (2, 'keep');
INSERT INTO lock_upgrade_test VALUES (3, 'also_keep');

-- Explicit transaction: BEGIN forces the same txn across statements
BEGIN;
SELECT id, val FROM lock_upgrade_test WHERE id = 1;
UPDATE lock_upgrade_test SET val = 'upgraded' WHERE id = 1;
SELECT id, val FROM lock_upgrade_test;
COMMIT;

-- Verify the update persisted after commit
SELECT id, val FROM lock_upgrade_test;

DROP TABLE lock_upgrade_test;
