CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR, code VARCHAR);

INSERT INTO products VALUES (1, 'Apple iPhone 14', 'AIP-14-X');
INSERT INTO products VALUES (2, 'Samsung Galaxy S23', 'SGL-S23-U');
INSERT INTO products VALUES (3, 'apple macOS', 'AMC-OS-10');
INSERT INTO products VALUES (4, 'Google Pixel 8', 'GPL-8-P');

-- Exact string match
SELECT name FROM products WHERE name LIKE 'Apple iPhone 14';

-- Prefix match
SELECT name FROM products WHERE name LIKE 'Apple%';

-- Case-insensitive prefix match
SELECT name FROM products WHERE name ILIKE 'apple%';

-- Middle match
SELECT name FROM products WHERE name LIKE '%Galaxy%';

-- Suffix match
SELECT name FROM products WHERE name LIKE '%8';

-- Single character wildcard
SELECT name FROM products WHERE code LIKE 'A_P-%';

-- Inversion logic
SELECT name FROM products WHERE name NOT ILIKE '%apple%';

DROP TABLE products;
