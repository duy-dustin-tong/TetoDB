CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR UNIQUE, age INT);

-- Valid inserts
INSERT INTO users VALUES (1, 'alice@test.com', 25);
INSERT INTO users VALUES (2, 'bob@test.com', 30);

-- Duplicate unique insert (should throw runtime error)
INSERT INTO users VALUES (3, 'alice@test.com', 40);

-- Duplicate unique update (should throw runtime error)
UPDATE users SET email = 'bob@test.com' WHERE id = 1;

-- Valid update
UPDATE users SET age = 26 WHERE id = 1;

-- Output should just be alice and bob with unmodified emails
SELECT * FROM users;

DROP TABLE users;
