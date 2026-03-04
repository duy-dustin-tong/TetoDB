-- test_3_10_views.sql
CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR, salary INT, department VARCHAR);

INSERT INTO employees VALUES (1, 'Alice', 60000, 'Engineering');
INSERT INTO employees VALUES (2, 'Bob', 50000, 'Marketing');
INSERT INTO employees VALUES (3, 'Charlie', 75000, 'Engineering');
INSERT INTO employees VALUES (4, 'David', 45000, 'Sales');

-- Create an Engineering View
CREATE VIEW engineering_emps AS SELECT id, name FROM employees WHERE department = 'Engineering';

-- Query the View Directly
SELECT * FROM engineering_emps;

-- Join with the View
CREATE TABLE projects (proj_id INT, emp_id INT, proj_name VARCHAR);
INSERT INTO projects VALUES (101, 1, 'Project Alpha');
INSERT INTO projects VALUES (102, 3, 'Project Beta');

SELECT p.proj_name, e.name FROM projects p JOIN engineering_emps e ON p.emp_id = e.id;

-- Drop the View
DROP VIEW engineering_emps;

-- This should fail since the view was dropped
SELECT * FROM engineering_emps;
