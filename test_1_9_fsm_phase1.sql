-- Phase 1: Create table and insert data BEFORE restart
CREATE TABLE fsm_test (id INTEGER PRIMARY KEY, data VARCHAR);
INSERT INTO fsm_test VALUES (1, 'alpha');
INSERT INTO fsm_test VALUES (2, 'beta');
INSERT INTO fsm_test VALUES (3, 'gamma');
SELECT id, data FROM fsm_test;
