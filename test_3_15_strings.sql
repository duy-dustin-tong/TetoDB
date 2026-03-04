CREATE TABLE users (id INTEGER PRIMARY KEY, first_name VARCHAR, last_name VARCHAR);

INSERT INTO users VALUES (1, 'john', 'DOE');
INSERT INTO users VALUES (2, 'aLiCe', 'sMiTh');
INSERT INTO users VALUES (3, 'Bob', NULL);
INSERT INTO users VALUES (4, 'Eve', 'Johnson');

-- UPPER and LOWER
SELECT id, UPPER(first_name), LOWER(last_name) FROM users;

-- LENGTH
SELECT id, first_name, LENGTH(first_name) FROM users;

-- CONCAT
SELECT id, CONCAT(first_name, ' ', last_name) FROM users;

-- SUBSTRING
SELECT id, first_name, SUBSTRING(first_name, 1, 3) FROM users;
SELECT id, SUBSTRING(first_name, 2, 2) FROM users;

-- NULL semantics (Should return NULL)
SELECT id, UPPER(last_name), LENGTH(last_name) FROM users WHERE id = 3;

DROP TABLE users;
