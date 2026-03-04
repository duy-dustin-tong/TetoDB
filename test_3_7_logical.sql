CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER, role VARCHAR);

INSERT INTO users VALUES (1, 20, 'admin');
INSERT INTO users VALUES (2, 17, 'user');
INSERT INTO users VALUES (3, 25, 'user');
INSERT INTO users VALUES (4, 30, 'mod');
INSERT INTO users VALUES (5, 65, 'guest');

SELECT id, age, role FROM users WHERE age >= 18 AND role = 'user';

SELECT id, age, role FROM users WHERE age < 18 OR age >= 65;

SELECT id, role FROM users WHERE NOT role = 'admin';

SELECT id, age FROM users WHERE age BETWEEN 20 AND 30;

SELECT id, age FROM users WHERE age NOT BETWEEN 20 AND 30;

SELECT id, role FROM users WHERE role IN ('admin', 'mod');

SELECT id, role FROM users WHERE role NOT IN ('admin', 'mod');

SELECT id, age, role FROM users WHERE role = 'admin' OR role = 'user' AND age < 20;

DROP TABLE users;
