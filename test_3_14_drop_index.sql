CREATE TABLE drop_index_test (
    id INT PRIMARY KEY,
    name VARCHAR
);

CREATE INDEX test_idx ON drop_index_test(name);

INSERT INTO drop_index_test VALUES (1, 'Alice');
INSERT INTO drop_index_test VALUES (2, 'Bob');

-- Verify basic query (IndexScan should trigger naturally later)
SELECT * FROM drop_index_test WHERE name = 'Alice';

-- Drop the index dynamically
DROP INDEX test_idx;

-- Ensure table is still accessible natively (SeqScan falback)
SELECT * FROM drop_index_test WHERE name = 'Bob';

-- Clear environment
DROP TABLE drop_index_test;
