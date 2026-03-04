CREATE TABLE parents (id INTEGER PRIMARY KEY, name VARCHAR);
INSERT INTO parents VALUES (1, 'ParentA');
INSERT INTO parents VALUES (2, 'ParentB');
CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, label VARCHAR, FOREIGN KEY (parent_id) REFERENCES parents (id) ON DELETE CASCADE);
INSERT INTO children VALUES (10, 1, 'Child1');
INSERT INTO children VALUES (20, 2, 'Child2');
SELECT id, parent_id, label FROM children;
DROP TABLE children;
DROP TABLE parents;
