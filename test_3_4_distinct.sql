CREATE TABLE distinct_test (
    id INT PRIMARY KEY,
    name VARCHAR,
    department VARCHAR
);

INSERT INTO distinct_test VALUES (1, 'Alice', 'Engineering');
INSERT INTO distinct_test VALUES (2, 'Bob', 'Engineering');
INSERT INTO distinct_test VALUES (3, 'Charlie', 'Sales');
INSERT INTO distinct_test VALUES (4, 'David', 'Sales');
INSERT INTO distinct_test VALUES (5, 'Eve', 'HR');

-- Query 1: Returns all 5 departments (including duplicates)
SELECT department FROM distinct_test;

-- Query 2: Returns only 3 unique departments
SELECT DISTINCT department FROM distinct_test;

-- Query 3: Multi-column DISTINCT filtering
INSERT INTO distinct_test VALUES (6, 'Frank', 'HR');
INSERT INTO distinct_test VALUES (7, 'Frank', 'HR'); -- duplicate values except ID
SELECT DISTINCT name, department FROM distinct_test;

DROP TABLE distinct_test;
