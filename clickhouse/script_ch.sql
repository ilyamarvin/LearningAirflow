create user airflow IDENTIFIED WITH sha256_password BY 'airflow';

grant select, insert on *.* to airflow;

CREATE TABLE default.assembly_task_issued
(
    `rid` String,
    `shk_id` Int64,
    `chrt_id` UInt32,
    `nm_id` UInt32,
    `as_id` UInt32,
    `wh_id` UInt16,
    `issued_dt` DateTime,
    `entry` LowCardinality(String)
)
ENGINE = MergeTree()
PARTITION BY toStartOfWeek(issued_dt, 1)
ORDER BY shk_id
TTL toStartOfWeek(issued_dt, 1) + toIntervalWeek(14)
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 72000, ttl_only_drop_parts = 1;

create database report;

create table if not exists report.issued_qty (
    dt_date Date,
    wh_id UInt16,
    qty_issued UInt32,
    dt_load DateTime materialized now()
)
engine = ReplacingMergeTree()
partition by toYYYYMM(dt_date)
order by (dt_date, wh_id)
ttl dt_date + interval 30 day;