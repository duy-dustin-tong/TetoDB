CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR,
    department VARCHAR,
    salary INT
);

INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000);
INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 85000);
INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 50000);
INSERT INTO employees VALUES (4, 'David', 'Sales', 45000);
INSERT INTO employees VALUES (5, 'Eve', 'HR', 70000);
INSERT INTO employees VALUES (6, 'Frank', 'HR', 65000);
INSERT INTO employees VALUES (7, 'Grace', 'Executive', 250000);

-- Query 1: Baseline GROUP BY without HAVING
-- Expected: Engineering (175k), Sales (95k), HR (135k), Executive (250k)
SELECT department, SUM(salary) FROM employees GROUP BY department;

-- Query 2: GROUP BY with HAVING to filter low total salaries
-- Expected: Engineering (175k), HR (135k), Executive (250k)
SELECT department, SUM(salary) FROM employees GROUP BY department HAVING SUM(salary) > 100000;

-- Query 3: Aliased aggregate functions used within HAVING 
-- Expected: HR (2 records)
SELECT department, COUNT(*) AS count_num FROM employees GROUP BY department HAVING count_num >= 2 AND department = 'HR';

DROP TABLE employees;
