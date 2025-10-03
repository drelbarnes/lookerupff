view: up_airtable_reports {
  derived_table: {
    sql:
    SELECT
      tx_id,
      alt_title_code,
      channel,
      contract,
      date,
      duration,
      end_time,
      product_code,
      product_title,
      program_id,
      requires_special_attention,
      series_title,
      start_time,
      tms_id,
      transmission_duration
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY date DESC, start_time DESC) AS rn
      FROM customers.up_airtable_reports
    ) t
    WHERE rn = 1 ;;

    persist_for: "24 hours"

    # Redshift-specific options
    distribution: "EVEN"
    sortkeys: ["date","start_time"]
  }


  dimension: tx_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.tx_id ;;
  }

  dimension: alt_title_code { type: string sql: ${TABLE}.alt_title_code ;; }
  dimension: channel { type: string sql: ${TABLE}.channel ;; }
  dimension: contract { type: string sql: ${TABLE}.contract ;; }
  dimension: date { type: string sql: ${TABLE}.date ;; }
  dimension: duration { type: string sql: ${TABLE}.duration ;; }
  dimension: end_time { type: string sql: ${TABLE}.end_time ;; }
  dimension: product_code { type: string sql: ${TABLE}.product_code ;; }
  dimension: product_title { type: string sql: ${TABLE}.product_title ;; }
  dimension: program_id { type: number sql: ${TABLE}.program_id ;; }
  dimension: requires_special_attention { type: string sql: ${TABLE}.requires_special_attention ;; }
  dimension: series_title { type: string sql: ${TABLE}.series_title ;; }
  dimension: start_time { type: string sql: ${TABLE}.start_time ;; }
  dimension: tms_id { type: string sql: ${TABLE}.tms_id ;; }
  dimension: transmission_duration { type: string sql: ${TABLE}.transmission_duration ;; }

  measure: count {
    type: count
    drill_fields: [tx_id]
  }
}
