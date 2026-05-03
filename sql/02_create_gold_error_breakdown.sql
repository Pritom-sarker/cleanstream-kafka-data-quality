CREATE OR REPLACE TABLE {GOLD_ERROR_TABLE} AS

WITH all_errors AS (

    SELECT
        kafka_partition,
        kafka_offset,
        raw_json,
        TRIM(single_error) AS error_type

    FROM {SILVER_CLEAN_TABLE}

    LATERAL VIEW EXPLODE(SPLIT(final_error_summary, ' , ')) error_table AS single_error

    WHERE final_error_summary IS NOT NULL
      AND TRIM(final_error_summary) != ''
),

error_count_data AS (

    SELECT
        error_type,
        COUNT(*) AS error_count,
        COUNT(DISTINCT kafka_partition) AS affected_partitions,
        MIN(kafka_offset) AS first_seen_offset,
        MAX(kafka_offset) AS last_seen_offset,
        FIRST(raw_json) AS sample_raw_json

    FROM all_errors

    WHERE error_type IS NOT NULL
      AND TRIM(error_type) != ''

    GROUP BY error_type
),

total_error_data AS (

    SELECT
        SUM(error_count) AS total_error_count
    FROM error_count_data
)

SELECT
    e.error_type,
    e.error_count,

    ROUND(e.error_count * 100.0 / t.total_error_count, 2) AS error_percentage,

    e.affected_partitions,
    e.first_seen_offset,
    e.last_seen_offset,
    e.sample_raw_json,

    CURRENT_TIMESTAMP() AS gold_processed_time

FROM error_count_data e
CROSS JOIN total_error_data t

ORDER BY e.error_count DESC
