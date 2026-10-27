DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS s_left;
DROP TABLE IF EXISTS s_right;

CREATE TABLE t_left (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE s_left (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE s_right (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_left SELECT number, number FROM numbers(100);
INSERT INTO t_right SELECT number, number FROM numbers(100);
INSERT INTO s_left SELECT number FROM numbers(10);
INSERT INTO s_right SELECT number FROM numbers(20);

SELECT count()
FROM
(
    SELECT * FROM t_left WHERE id IN (SELECT id FROM s_left)
    UNION ALL
    SELECT * FROM t_right WHERE id IN (SELECT id FROM s_right)
)
SETTINGS max_threads = 2;

SELECT count()
FROM
(
    SELECT * FROM t_left WHERE id IN (SELECT id FROM s_left WHERE throwIf(1) = 0)
    UNION ALL
    SELECT * FROM t_right WHERE id IN (SELECT id FROM s_right)
)
SETTINGS max_threads = 2; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT count()
FROM
(
    SELECT * FROM t_left WHERE id IN (SELECT number FROM numbers(2000000))
    UNION ALL
    SELECT * FROM t_right WHERE id IN (SELECT number FROM numbers(2000000))
)
SETTINGS max_threads = 2, max_memory_usage = 1000000; -- { serverError MEMORY_LIMIT_EXCEEDED }

DROP TABLE t_left;
DROP TABLE t_right;
DROP TABLE s_left;
DROP TABLE s_right;
