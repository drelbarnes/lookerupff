view: roku {
  derived_table: {
    sql: WITH last_days AS (
    SELECT dateadd(day, -seq, current_date) AS end_date
    FROM (
        SELECT row_number() OVER() AS seq
        FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
              UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
              UNION ALL SELECT 8 UNION ALL SELECT 9) t
    ) x
)

SELECT
    ld.end_date as report_date,
    (
        SELECT COUNT(DISTINCT user_id)
        FROM vimeo_ott_webhook.customer_product_renewed r
        WHERE r.platform = 'roku'
          AND DATE(r.timestamp) BETWEEN dateadd(day, -31, ld.end_date) AND ld.end_date
    ) +
    (
        SELECT COUNT(DISTINCT user_id)
        FROM vimeo_ott_webhook.customer_product_free_trial_converted c
        WHERE c.platform = 'roku'
          AND c.subscription_frequency = 'yearly'
          AND DATE(c.timestamp) BETWEEN dateadd(year, -1, ld.end_date) AND ld.end_date
    ) AS total
FROM last_days ld


;;
  }
}
