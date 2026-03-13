# TetoDB SQL Reference

This document describes the SQL dialect supported by TetoDB.

## DDL

### CREATE TABLE

```sql
CREATE TABLE table_name (
  column_name data_type [PRIMARY KEY] [NOT NULL] [UNIQUE],
  ...,
  [FOREIGN KEY (child_col) REFERENCES parent_table (parent_col)
    [ON DELETE CASCADE | SET NULL | RESTRICT]
    [ON UPDATE CASCADE | SET NULL | RESTRICT]
  ]
);
```

Supported constraints:

- `PRIMARY KEY`
- `NOT NULL`
- `UNIQUE`
- `FOREIGN KEY` with `CASCADE`, `SET NULL`, `RESTRICT`

### CREATE INDEX

```sql
CREATE [UNIQUE] INDEX index_name ON table_name (column1, column2, ...);
```

### CREATE VIEW

```sql
CREATE VIEW view_name AS
SELECT ...;
```

### DROP

```sql
DROP TABLE table_name;
DROP INDEX index_name;
DROP VIEW view_name;
```

## Data Types

- `INT` / `INTEGER`
- `BIGINT`
- `SMALLINT`
- `TINYINT`
- `BOOLEAN` / `BOOL`
- `DECIMAL` / `FLOAT` / `DOUBLE`
- `VARCHAR` / `TEXT`
- `CHAR`
- `DATE` / `TIMESTAMP`

Timestamp/date input accepted formats:

- `YYYY-MM-DD`
- `YYYY-MM-DD HH:MM:SS`

Examples:

```sql
INSERT INTO events VALUES (1, 'Meeting', '2024-06-15');
INSERT INTO logs VALUES (1, '2024-06-15 14:30:00');
```

Escaped single quote in strings:

```sql
INSERT INTO users VALUES (1, 'O''Brien');
```

## DML

### SELECT

```sql
[WITH cte_alias AS (SELECT ...), ...]
SELECT [DISTINCT] * | expr [AS alias], ...
FROM table_name | (subquery)
[JOIN table_name ON condition]
[WHERE boolean_expression]
[GROUP BY expression, ...]
[HAVING boolean_expression]
[ORDER BY expression [ASC | DESC]]
[LIMIT number]
[OFFSET number];
```

Set operations:

- `UNION [ALL]`
- `INTERSECT [ALL]`
- `EXCEPT [ALL]`

### INSERT

```sql
INSERT INTO table_name VALUES
  (...),
  (...);
```

Important: inserts are positional; column-targeted insert syntax is not supported.

### UPDATE

```sql
UPDATE table_name
SET column1 = expression, column2 = expression
[WHERE boolean_expression];
```

### DELETE

```sql
DELETE FROM table_name
[WHERE boolean_expression];
```

## Expressions And Operators

Arithmetic:

- `+`, `-`, `*`, `/`

Comparisons:

- `=`, `!=`, `<`, `<=`, `>`, `>=`
- `IS NULL`, `IS NOT NULL`
- `[NOT] LIKE`, `[NOT] ILIKE`
- `[NOT] IN (...)`
- `[NOT] BETWEEN ... AND ...`

Logical:

- `AND`, `OR`, `NOT`

Built-in functions:

- Aggregates: `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `MEDIAN`
- String: `UPPER`, `LOWER`, `LENGTH`, `CONCAT`, `SUBSTRING`

## Transactions

```sql
BEGIN;
COMMIT;
ROLLBACK;
SAVEPOINT sp_name;
RELEASE SAVEPOINT sp_name;
ROLLBACK TO sp_name;
```

## EXPLAIN

Use `EXPLAIN` to print the physical plan tree without executing data modifications.

```sql
EXPLAIN SELECT * FROM users WHERE id = 1;
```
