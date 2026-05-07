#!/usr/bin/env bash
set -e

DB="bigdata_hw"
SORTED_TABLE="logs_sorted"
UNSORTED_TABLE="logs_unsorted"

# 安全目标：每张表 55GB 未压缩数据，两张表合计约 110GB
TARGET_PER_TABLE_GB=55
TARGET_PER_TABLE_BYTES=$((TARGET_PER_TABLE_GB * 1024 * 1024 * 1024))

# 每轮插入 25 万行，比较稳，不容易一下子吃太多空间
ROWS_PER_CHUNK=250000
HASH_PARTS=128

if [ -z "${CH_PASS:-}" ]; then
  read -s -p "ClickHouse password: " CH_PASS
  echo
fi

run_query() {
  clickhouse-client --password="$CH_PASS" --query="$1"
}

echo "Checking database and tables..."

run_query "CREATE DATABASE IF NOT EXISTS ${DB};"

run_query "
CREATE TABLE IF NOT EXISTS ${DB}.${SORTED_TABLE}
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
"

run_query "
CREATE TABLE IF NOT EXISTS ${DB}.${UNSORTED_TABLE}
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
"

# 如果旧表没有 payload 字段，则补上；如果已有，不会重复添加
run_query "ALTER TABLE ${DB}.${SORTED_TABLE} ADD COLUMN IF NOT EXISTS payload String DEFAULT '';"
run_query "ALTER TABLE ${DB}.${UNSORTED_TABLE} ADD COLUMN IF NOT EXISTS payload String DEFAULT '';"

get_table_bytes() {
  local table_name="$1"
  clickhouse-client --password="$CH_PASS" --query="
  SELECT toUInt64(ifNull(sum(data_uncompressed_bytes), 0))
  FROM system.parts
  WHERE database = '${DB}'
    AND table = '${table_name}'
    AND active;
  "
}

get_table_rows() {
  local table_name="$1"
  clickhouse-client --password="$CH_PASS" --query="
  SELECT count()
  FROM ${DB}.${table_name};
  "
}

print_size() {
  clickhouse-client --password="$CH_PASS" --query="
  SELECT
      table,
      sum(rows) AS rows,
      formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
      formatReadableSize(sum(data_compressed_bytes)) AS compressed,
      formatReadableSize(sum(bytes_on_disk)) AS on_disk
  FROM system.parts
  WHERE database = '${DB}'
    AND table IN ('${SORTED_TABLE}', '${UNSORTED_TABLE}')
    AND active
  GROUP BY table
  ORDER BY table;
  "
}

insert_chunk() {
  local table_name="$1"
  local offset="$2"

  echo "Insert into ${table_name}, offset=${offset}, rows=${ROWS_PER_CHUNK}"

  clickhouse-client --password="$CH_PASS" --query="
  INSERT INTO ${DB}.${table_name}
  (
      event_time,
      user_id,
      event_type,
      page,
      device,
      country,
      bytes,
      status,
      payload
  )
  SELECT
      toDateTime('2026-01-01 00:00:00') + toIntervalSecond(intDiv(n, 1000)) AS event_time,

      toUInt64(1 + modulo(cityHash64(n), 500000)) AS user_id,

      arrayElement(
          ['view', 'click', 'scroll', 'purchase'],
          1 + modulo(cityHash64(n, 1), 4)
      ) AS event_type,

      concat(
          '/category/',
          toString(1 + modulo(cityHash64(n, 2), 1000)),
          '/product/',
          toString(1 + modulo(cityHash64(n, 3), 5000000)),
          '/details?ref=',
          toString(cityHash64(n, 4)),
          '&src=ad_campaign_',
          toString(1 + modulo(cityHash64(n, 5), 9999))
      ) AS page,

      arrayElement(
          ['mobile', 'desktop', 'tablet'],
          1 + modulo(cityHash64(n, 6), 3)
      ) AS device,

      arrayElement(
          ['RU', 'CN', 'US', 'DE'],
          1 + modulo(cityHash64(n, 7), 4)
      ) AS country,

      toUInt32(200 + modulo(cityHash64(n, 8), 9801)) AS bytes,

      arrayElement(
          [200, 200, 200, 404, 500],
          1 + modulo(cityHash64(n, 9), 5)
      ) AS status,

      arrayStringConcat(
          arrayMap(
              i -> hex(cityHash64(concat(toString(n), '-', toString(i)))),
              range(${HASH_PARTS})
          ),
          ''
      ) AS payload

  FROM
  (
      SELECT number + ${offset} AS n
      FROM numbers(${ROWS_PER_CHUNK})
  );
  "
}

echo
echo "Initial table size:"
print_size

while true; do
  sorted_bytes=$(get_table_bytes "${SORTED_TABLE}")
  unsorted_bytes=$(get_table_bytes "${UNSORTED_TABLE}")

  echo
  echo "Current size:"
  print_size

  if [ "$sorted_bytes" -ge "$TARGET_PER_TABLE_BYTES" ] && [ "$unsorted_bytes" -ge "$TARGET_PER_TABLE_BYTES" ]; then
    echo
    echo "Target reached: each table is at least ${TARGET_PER_TABLE_GB}GB uncompressed."
    break
  fi

  current_rows=$(get_table_rows "${SORTED_TABLE}")
  offset=$((current_rows + 1000000000))

  if [ "$sorted_bytes" -lt "$TARGET_PER_TABLE_BYTES" ]; then
    insert_chunk "${SORTED_TABLE}" "$offset"
  fi

  if [ "$unsorted_bytes" -lt "$TARGET_PER_TABLE_BYTES" ]; then
    insert_chunk "${UNSORTED_TABLE}" "$offset"
  fi
done

echo
echo "Final table size:"
print_size

echo
echo "Final row counts:"
clickhouse-client --password="$CH_PASS" --query="
SELECT '${SORTED_TABLE}' AS table, count() AS rows FROM ${DB}.${SORTED_TABLE}
UNION ALL
SELECT '${UNSORTED_TABLE}' AS table, count() AS rows FROM ${DB}.${UNSORTED_TABLE};
"
