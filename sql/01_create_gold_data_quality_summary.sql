CREATE OR REPLACE TABLE {GOLD_SUMMARY_TABLE} AS

SELECT
    COUNT(*) AS total_events,

    SUM(CASE WHEN final_error_summary IS NULL OR TRIM(final_error_summary) = '' THEN 1 ELSE 0 END) AS valid_events,

    SUM(CASE WHEN final_error_summary IS NOT NULL AND TRIM(final_error_summary) != '' THEN 1 ELSE 0 END) AS invalid_events,

    ROUND(
        SUM(CASE WHEN final_error_summary IS NULL OR TRIM(final_error_summary) = '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS valid_rate,

    ROUND(
        SUM(CASE WHEN final_error_summary IS NOT NULL AND TRIM(final_error_summary) != '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS invalid_rate,

    SUM(CASE WHEN schema_drift_exists = true THEN 1 ELSE 0 END) AS schema_drift_count,

    ROUND(
        SUM(CASE WHEN schema_drift_exists = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS schema_drift_rate,

    SUM(CASE WHEN parsed_event_id IS NULL OR TRIM(parsed_event_id) = '' THEN 1 ELSE 0 END) AS missing_event_id_count,

    SUM(CASE WHEN parsed_customer_id IS NULL OR TRIM(CAST(parsed_customer_id AS STRING)) = '' THEN 1 ELSE 0 END) AS missing_customer_id_count,

    SUM(CASE WHEN parsed_amount IS NULL OR TRIM(CAST(parsed_amount AS STRING)) = '' THEN 1 ELSE 0 END) AS missing_amount_count,

    SUM(CASE WHEN parsed_payment_status IS NULL OR TRIM(parsed_payment_status) = '' THEN 1 ELSE 0 END) AS missing_payment_status_count,

    SUM(
        CASE 
            WHEN parsed_amount IS NOT NULL 
             AND TRIM(CAST(parsed_amount AS STRING)) != ''
             AND clean_parsed_amount IS NULL
            THEN 1 ELSE 0 
        END
    ) AS invalid_amount_count,

    SUM(
        CASE 
            WHEN parsed_payment_status IS NOT NULL
             AND TRIM(parsed_payment_status) != ''
             AND LOWER(parsed_payment_status) NOT IN ('success', 'failed', 'pending')
            THEN 1 ELSE 0 
        END
    ) AS invalid_payment_status_count,

    SUM(CASE WHEN business_validation_errors LIKE '%amount is negative%' THEN 1 ELSE 0 END) AS negative_amount_count,

    SUM(CASE WHEN business_validation_errors LIKE '%event_time is in the future%' THEN 1 ELSE 0 END) AS future_event_time_count,

    COUNT(DISTINCT kafka_partition) AS kafka_partition_count,

    MIN(kafka_offset) AS min_kafka_offset,
    MAX(kafka_offset) AS max_kafka_offset,

    CURRENT_TIMESTAMP() AS gold_processed_time

FROM {SILVER_CLEAN_TABLE}
