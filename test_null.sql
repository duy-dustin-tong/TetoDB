CREATE TABLE null_test (id INTEGER, val INTEGER);
INSERT INTO null_test VALUES (1, NULL);
INSERT INTO null_test VALUES (2, 20);
INSERT INTO null_test VALUES (3, NULL);

-- Should return 1 and 3
SELECT id, val FROM null_test WHERE val IS NULL;

-- Should return 2
SELECT id, val FROM null_test WHERE val IS NOT NULL;

-- Check exact fetching
SELECT id, val FROM null_test WHERE id = 1;

DROP TABLE null_test;
