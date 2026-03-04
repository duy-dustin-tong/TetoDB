CREATE TABLE ltest (id INTEGER, val VARCHAR);
INSERT INTO ltest VALUES (1, 'alpha');
INSERT INTO ltest VALUES (2, 'beta');
UPDATE ltest SET val = 'gamma' WHERE id = 1;
SELECT id, val FROM ltest;
DELETE FROM ltest WHERE id = 2;
SELECT id, val FROM ltest;
DROP TABLE ltest;
