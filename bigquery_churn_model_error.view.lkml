explore: bigquery_churn_model_error {}
view: bigquery_churn_model_error {
  derived_table: {
    sql: SELECT user_id,timestamp, 1 as error FROM javascript.error where user_id is not null
  UNION ALL
  SELECT user_id,timestamp, 1 as error FROM android.error where user_id is not null
  UNION ALL
  SELECT user_id,timestamp, 1 as error FROM ios.error where user_id is not null ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: number
    sql: safe_cast(${TABLE}.user_id as int64) ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  measure: error_count {
    type: sum
    sql: ${TABLE}.error ;;
  }

}
