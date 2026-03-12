# TetoDB SQL Syntax Reference

This document outlines the complete SQL dialect supported by the custom TetoDB parser and execution engine. 

While largely inspired by PostgreSQL, TetoDB supports a curated subset of SQL focused on core relational algebra features, complete with CTEs, set operations, nested transactions, and complex query execution.

---

## Data Definition Language (DDL)

### 1. CREATE TABLE
Define a new table with its schema, constraints, and foreign keys.

```sql
CREATE TABLE table_name (
    column1_name data_type [PRIMARY KEY] [NOT NULL] [UNIQUE],
    column2_name data_type,
    ...
    [FOREIGN KEY (child_column) REFERENCES parent_table (parent_column) 
        [ON DELETE CASCADE | SET NULL | RESTRICT] 
        [ON UPDATE CASCADE | SET NULL | RESTRICT]
    ]
);
```
**Supported Constraints:**
* `PRIMARY KEY`: Marks the column as the primary index key (implicitly `NOT NULL`).
* `NOT NULL`: Forbids null insertions into this column.
* `UNIQUE`: Enforces global uniqueness across the column.
* `FOREIGN KEY`: Establishes referential integrity. Supports `CASCADE`, `SET NULL`, and `RESTRICT` behaviors.

*Example:*
```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50) NOT NULL);
```

### 2. CREATE INDEX
Creates a supplementary B+Tree index to speed up lookups or enforce multi-column constraints.

```sql
CREATE [UNIQUE] INDEX index_name ON table_name (column1, column2, ...);
```

### 3. CREATE VIEW
Creates a virtual table based on the result-set of an underlying SELECT statement.

```sql
CREATE VIEW view_name AS
    SELECT ...;
```

### 4. DROP Statements
Safely removes metadata and backend data constructs from the Catalog.

```sql
DROP TABLE table_name;
DROP INDEX index_name;
DROP VIEW view_name;
```

---

## Supported Data Types

| SQL Type | Internal Type | Size | Notes |
|----------|--------------|------|-------|
| `INT` / `INTEGER` | INTEGER | 4 bytes | 32-bit signed integer |
| `BIGINT` | BIGINT | 8 bytes | 64-bit signed integer |
| `SMALLINT` | SMALLINT | 2 bytes | 16-bit signed integer |
| `TINYINT` | TINYINT | 1 byte | 8-bit signed integer |
| `BOOLEAN` / `BOOL` | BOOLEAN | 1 byte | `TRUE` or `FALSE` |
| `DECIMAL` / `FLOAT` / `DOUBLE` | DECIMAL | 8 bytes | 64-bit double precision |
| `VARCHAR` / `TEXT` | VARCHAR | Variable | Length-prefixed string |
| `CHAR` | CHAR | Variable | Length-prefixed string |
| `DATE` / `TIMESTAMP` | TIMESTAMP | 8 bytes | UTC epoch seconds. Accepts `'YYYY-MM-DD'` or `'YYYY-MM-DD HH:MM:SS'` |

### Timestamp/Date Literals
```sql
INSERT INTO events VALUES (1, 'Meeting', '2024-06-15');
INSERT INTO logs VALUES (1, '2024-06-15 14:30:00');
```
String values are automatically coerced to TIMESTAMP when inserting/updating into TIMESTAMP columns.

### Escaped Single Quotes
Use `''` (two single quotes) to represent a literal single quote inside a string:
```sql
INSERT INTO users VALUES (1, 'O''Brien');
SELECT * FROM users WHERE name = 'it''s';
```

---

## Data Manipulation Language (DML)

### 1. SELECT (Querying Data)
TetoDB provides a rich query syntax encompassing Common Table Expressions (CTEs), Subqueries, Joins, Groupings, and Output formatting.

**Syntax Hierarchy:**
```sql
[WITH cte_alias AS (SELECT ...), ...]
SELECT [DISTINCT] * | column_name [AS alias], function(), expression
FROM table_name | (sub-query)
[JOIN table_name ON condition]
[WHERE boolean_expression]
[GROUP BY expression1, expression2, ...]
[HAVING boolean_expression]
[ORDER BY expression [ASC | DESC]]
[LIMIT number]
[OFFSET number]
```

**Set Operations:**
You can chain multiple `SELECT` results using set algebra:
* `UNION [ALL]`
* `INTERSECT [ALL]`
* `EXCEPT [ALL]`

### 2. INSERT
Append new rows into a table. TetoDB expects full-row continuous value lists. Explicit column targeting is bypassed; values must align with table creation definition strictly.

```sql
INSERT INTO table_name VALUES
    (value1, value2...),
    (value3, value4...);
```

### 3. UPDATE
Modify existing tuples mapping to specific conditions. Evaluator fully supports mathematical formulas within the `SET` block.

```sql
UPDATE table_name 
SET column1 = expression, column2 = expression
[WHERE boolean_expression];
```

### 4. DELETE
Remove subsets of rows that match a specified boolean condition.

```sql
DELETE FROM table_name
[WHERE boolean_expression];
```

---

## Expressions & Operators

TetoDB executes an internal recursive evaluation tree for scalar values. 

**Mathematical Operators:**
* `+` (Add)
* `-` (Subtract / Unary negation)
* `*` (Multiply)
* `/` (Divide)

**Comparisons:**
* `=`, `>`, `<`, `>=`, `<=`, `!=`
* `IS NULL`, `IS NOT NULL`
* `[NOT] LIKE`, `[NOT] ILIKE` (Wildcard string matching via `%` and `_`)
* `[NOT] IN (val1, val2...)` or `[NOT] IN (SELECT ...)`
* `[NOT] BETWEEN lower_bound AND upper_bound`

**Logical Operators:**
* `AND`, `OR`, `NOT`

### Built-In Functions
Functions operate on rows synchronously:
* **Aggregate Functions** (Used often with `GROUP BY`): 
  * `COUNT()`, `SUM()`, `MIN()`, `MAX()`, `AVG()`, `MEDIAN()`
* **Scalar String Functions**:
  * `UPPER(str)`, `LOWER(str)`, `LENGTH(str)`, `CONCAT(str1, str2, ...)`, `SUBSTRING(str, ...)`

---

## Transaction & Concurrency Control

TetoDB supports active locking structures and Write-Ahead Log durability parameters via explicit transaction boundaries.

```sql
BEGIN;
COMMIT;
ROLLBACK;
```

**Transaction Boundaries:**
```sql
BEGIN;
COMMIT;
ROLLBACK;
```

---

## Unsupported Features (SQLAlchemy Considerations)

Because TetoDB is a lightweight engine, certain standard SQL features commonly used by ORMs like SQLAlchemy are **explicitly not supported**. Attempting to use them will result in a `std::runtime_error`.

1. **No Nested Transactions / Savepoints:** `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT` are not supported and will throw an error. SQLAlchemy's `session.begin_nested()` will not work.
2. **No Prepared Statement Deallocation:** The `DEALLOCATE` command is not supported.
3. **No ALTER TABLE:** Schema modifications after creation are not supported.
4. **No Auto-Increment Sequences:** TetoDB does not automatically generate primary keys. You must insert PKs manually.
5. **Positional INSERTs Only:** `INSERT INTO table (col) VALUES (val)` is not supported; you must use `INSERT INTO table VALUES (val1, val2, ...)`.
6. **No Information Schema:** SQLAlchemy reflection (`metadata.reflect`) is limited.

---

## Debugging and Engine Utilities

### EXPLAIN
Prefix any DML statement with `EXPLAIN` to halt physical execution and instead retrieve the raw relational algebraic node tree projected by the Query Planner.

```sql
EXPLAIN SELECT u.name, SUM(o.total) 
FROM users u JOIN orders o ON u.id = o.user_id 
GROUP BY u.name;
```
