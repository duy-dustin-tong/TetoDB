CREATE TABLE types_test (id INTEGER, flag BOOLEAN, price DECIMAL, label VARCHAR);
INSERT INTO types_test VALUES (1, TRUE, 9.99, 'apple');
INSERT INTO types_test VALUES (2, FALSE, 0.50, 'banana');
INSERT INTO types_test VALUES (3, TRUE, 123.456, 'cherry');
INSERT INTO types_test VALUES (4, FALSE, NULL, 'date');

-- All rows
SELECT id, flag, price, label FROM types_test;

-- Filter by boolean
SELECT id, label FROM types_test WHERE flag IS NOT NULL;

-- Filter by decimal comparison
SELECT id, price FROM types_test WHERE price IS NOT NULL;

-- Check NULL interaction
SELECT id, price FROM types_test WHERE price IS NULL;

DROP TABLE types_test;
