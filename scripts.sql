CREATE DATABASE IF NOT EXISTS bigdata_hw;

DROP TABLE IF EXISTS bigdata_hw.logs_sorted;
DROP TABLE IF EXISTS bigdata_hw.logs_unsorted;

CREATE TABLE bigdata_hw.logs_sorted
(
    event_time DateTime,
    user_id UInt64,
    event_type String,
    page String,
    device String,
    country String,
    bytes UInt32,
    status UInt16,
    payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, event_time);

CREATE TABLE bigdata_hw.logs_unsorted
(
    event_time DateTime,
    user_id UInt64,
    event_type String,
    page String,
    device String,
    country String,
    bytes UInt32,
    status UInt16,
    payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY tuple();

-- Check final data size
SELECT
    table,
    sum(rows) AS rows,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    formatReadableSize(sum(bytes_on_disk)) AS on_disk
FROM system.parts
WHERE database = 'bigdata_hw'
  AND table IN ('logs_sorted', 'logs_unsorted')
  AND active
GROUP BY table
ORDER BY table;

-- SPU calculation
WITH 1800 AS delta_seconds
SELECT avg(session_cnt) AS spu
FROM
(
    SELECT
        user_id,
        countIf(is_new_session = 1) AS session_cnt
    FROM
    (
        SELECT
            user_id,
            event_time,
            if(
                rn = 1
                OR dateDiff('second', prev_time, event_time) > delta_seconds,
                1,
                0
            ) AS is_new_session
        FROM
        (
            SELECT
                user_id,
                event_time,
                row_number() OVER (
                    PARTITION BY user_id
                    ORDER BY event_time
                ) AS rn,
                lagInFrame(event_time, 1, event_time) OVER (
                    PARTITION BY user_id
                    ORDER BY event_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS prev_time
            FROM bigdata_hw.logs_sorted
        )
    )
    GROUP BY user_id
);

-- Aggregation query for sorted table
SELECT
    user_id,
    count() AS events,
    uniqExact(page) AS uniq_pages,
    sum(bytes) AS total_bytes
FROM bigdata_hw.logs_sorted
WHERE event_time >= '2026-01-01 00:00:00'
  AND event_time <  '2026-03-01 00:00:00'
  AND user_id BETWEEN 1000 AND 50000
GROUP BY user_id
FORMAT Null;

-- Aggregation query for unsorted table
SELECT
    user_id,
    count() AS events,
    uniqExact(page) AS uniq_pages,
    sum(bytes) AS total_bytes
FROM bigdata_hw.logs_unsorted
WHERE event_time >= '2026-01-01 00:00:00'
  AND event_time <  '2026-03-01 00:00:00'
  AND user_id BETWEEN 1000 AND 50000
GROUP BY user_id
FORMAT Null;

-- Query log
SYSTEM FLUSH LOGS;

SELECT
    query_duration_ms,
    read_rows,
    formatReadableSize(read_bytes) AS read_size,
    query
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '%bigdata_hw.logs_%'
ORDER BY event_time DESC
LIMIT 10;
