view: ovation_airtable_reports {

  derived_table: {
    sql:
      WITH ranked AS (
        SELECT
          alt_title_code,
          channel,
          contract,
          date,
          duration,
          end_time,
          product_code,
          product_title,
          CAST(program_id AS VARCHAR) AS program_id,   -- normalize type
          requires_special_attention,
          series_title,
          start_time,
          tms_id,
          transmission_duration,
          tx_id,
          ROW_NUMBER() OVER (
            PARTITION BY date, start_time, end_time, CAST(program_id AS VARCHAR)
            ORDER BY CAST(date AS DATE) ASC, start_time ASC
          ) AS rn
        FROM customers.ovation_airtable_reports
        WHERE CAST(date AS DATE) >= CURRENT_DATE
          AND CAST(date AS DATE) <= CURRENT_DATE + INTERVAL '90 day'
      )

      SELECT *
      FROM ranked
      WHERE rn = 1
      ORDER BY CAST(date AS DATE) ASC, start_time ASC
      ;;
  }

  # ---------- DIMENSIONS ----------

  dimension: tx_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.tx_id ;;
  }

  dimension: alt_title_code               { type: string sql: ${TABLE}.alt_title_code ;; }
  dimension: channel                      { type: string sql: ${TABLE}.channel ;; }
  dimension: contract                     { type: string sql: ${TABLE}.contract ;; }
  dimension: date                         { type: string sql: ${TABLE}.date ;; }
  dimension: duration                     { type: string sql: ${TABLE}.duration ;; }
  dimension: end_time                     { type: string sql: ${TABLE}.end_time ;; }
  dimension: product_code                 { type: string sql: ${TABLE}.product_code ;; }
  dimension: product_title                { type: string sql: ${TABLE}.product_title ;; }

  dimension: program_id {
    type: string
    sql: ${TABLE}.program_id ;;
  }

  dimension: requires_special_attention   { type: string sql: ${TABLE}.requires_special_attention ;; }
  dimension: series_title                 { type: string sql: ${TABLE}.series_title ;; }
  dimension: start_time                   { type: string sql: ${TABLE}.start_time ;; }
  dimension: tms_id                       { type: string sql: ${TABLE}.tms_id ;; }
  dimension: transmission_duration        { type: string sql: ${TABLE}.transmission_duration ;; }

  # ---------- MEASURES ----------

  measure: count {
    type: count
    drill_fields: [tx_id]
  }
}
