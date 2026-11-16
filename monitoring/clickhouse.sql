CREATE TABLE IF NOT EXISTS sales
(
    day Date,
    region UInt8,
    category UInt8,
    amount UInt32
)
ENGINE = MergeTree
ORDER BY (day, region, category)
AS SELECT
    toDate('2025-01-01') + toUInt16(number % 30) AS day,
    toUInt8(intDiv(number, 30) % 8) AS region,
    toUInt8(intDiv(number, 240) % 16) AS category,
    toUInt32(number % 10000) AS amount
FROM numbers(1000000);

CREATE TABLE IF NOT EXISTS sales_mart
(
    build_id String,
    day Date,
    region UInt8,
    category UInt8,
    orders UInt64,
    revenue UInt64
)
ENGINE = MergeTree
ORDER BY (build_id, day, region, category);
