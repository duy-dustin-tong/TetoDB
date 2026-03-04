CREATE TABLE departments (
    id INT PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR,
    dept_id INT,
    FOREIGN KEY (dept_id) REFERENCES departments (id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Valid Inserts
INSERT INTO departments VALUES (10, 'Engineering');
INSERT INTO departments VALUES (20, 'Sales');

INSERT INTO employees VALUES (1, 'Alice', 10);
INSERT INTO employees VALUES (2, 'Bob', 20);

-- Query 1: Valid initial state
SELECT * FROM employees;

-- Test 1: Invalid Child Update (Should trigger constraint violation because dept 30 does not exist)
UPDATE employees SET dept_id = 30 WHERE id = 1;

-- Test 2: Valid Parent Update Cascade (Should cascade and update Bob's dept_id to 21)
UPDATE departments SET id = 21 WHERE id = 20;

-- Query 2: Final state (Bob should be 21, Alice should still be 10)
SELECT * FROM employees;

DROP TABLE employees;
DROP TABLE departments;
