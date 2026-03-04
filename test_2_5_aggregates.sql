-- ============================================
-- TEST 2.5: Aggregate Keywords (MIN, MAX, MED)
-- ============================================
CREATE TABLE agg_test (id INTEGER PRIMARY KEY, val INTEGER, name VARCHAR);
INSERT INTO agg_test VALUES (1, 10, 'alpha');
INSERT INTO agg_test VALUES (2, 50, 'beta');
INSERT INTO agg_test VALUES (3, 30, 'gamma');
INSERT INTO agg_test VALUES (4, 20, 'delta');

-- Test all aggregate functions
SELECT COUNT(id) FROM agg_test;
SELECT SUM(val) FROM agg_test;
SELECT AVG(val) FROM agg_test;
SELECT MIN(val) FROM agg_test;
SELECT MAX(val) FROM agg_test;

-- Test with GROUP BY and ORDER BY (keywords also added)
SELECT name, SUM(val) FROM agg_test GROUP BY name;

-- Test ORDER BY with ASC/DESC keywords
SELECT id, val FROM agg_test ORDER BY val ASC;
SELECT id, val FROM agg_test ORDER BY val DESC;

DROP TABLE agg_test;
