CREATE TABLE employees (id INT, name VARCHAR(20), salary INT, department_id INT);
CREATE TABLE departments (id INT, name VARCHAR(20));

INSERT INTO employees VALUES (1, 'Alice', 60000, 1);
INSERT INTO employees VALUES (2, 'Bob',   50000, 1);
INSERT INTO employees VALUES (3, 'Charlie', 80000, 2);
INSERT INTO employees VALUES (4, 'Dave',  45000, 3);
INSERT INTO employees VALUES (5, 'Eve',   90000, 2);

INSERT INTO departments VALUES (1, 'HR');
INSERT INTO departments VALUES (2, 'Engineering');
INSERT INTO departments VALUES (3, 'Sales');

-- 1. Uncorrelated IN Subquery
SELECT id, name FROM employees WHERE department_id IN (SELECT id FROM departments WHERE name = 'Engineering');

-- 2. FROM Subquery
SELECT e.name, e.salary FROM (SELECT name, salary FROM employees WHERE salary > 50000) AS e;

-- 3. Common Table Expression (CTE)
WITH high_earners AS (
    SELECT name, salary, department_id FROM employees WHERE salary >= 70000
)
SELECT h.name, d.name AS dept_name 
FROM high_earners h 
JOIN departments d ON h.department_id = d.id;

DROP TABLE employees;
DROP TABLE departments;
