-- ============================================
-- TEST 2.3: Planner Arena Pointer Stability
-- ============================================
-- This test creates complex queries that force the planner
-- to allocate many expressions and plan nodes, which would
-- trigger vector reallocation and pointer invalidation if
-- still using std::vector.

CREATE TABLE plan_a (id INTEGER PRIMARY KEY, x INTEGER, name VARCHAR);
CREATE TABLE plan_b (id INTEGER PRIMARY KEY, y INTEGER, label VARCHAR);
INSERT INTO plan_a VALUES (1, 10, 'alpha');
INSERT INTO plan_a VALUES (2, 20, 'beta');
INSERT INTO plan_a VALUES (3, 30, 'gamma');
INSERT INTO plan_b VALUES (1, 100, 'one');
INSERT INTO plan_b VALUES (2, 200, 'two');
INSERT INTO plan_b VALUES (3, 300, 'three');

-- Complex SELECT: multiple columns + WHERE with AND (many expressions)
SELECT id, x, name FROM plan_a WHERE x > 5 AND x < 25;

-- JOIN query: creates join plan node + scan nodes + expressions
SELECT plan_a.id, plan_a.name, plan_b.label FROM plan_a JOIN plan_b ON plan_a.id = plan_b.id WHERE plan_a.x > 10;

-- Aggregation + GROUP BY: more plan nodes
SELECT x, COUNT(id) FROM plan_a GROUP BY x;

-- UPDATE with complex WHERE + multiple SET columns
UPDATE plan_a SET x = 99, name = 'updated' WHERE id = 1 AND x = 10;
SELECT id, x, name FROM plan_a;

-- DELETE with WHERE
DELETE FROM plan_a WHERE x = 20 AND name = 'beta';
SELECT id, x, name FROM plan_a;

DROP TABLE plan_b;
DROP TABLE plan_a;
