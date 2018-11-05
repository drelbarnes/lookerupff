view: bigquery_churn_model_removewatchlist {
  derived_table: {
    sql:
      SELECT user_id,timestamp FROM javascript.removewatchlist where user_id is not null
      UNION ALL
      SELECT user_id,timestamp FROM android.removewatchlist where user_id is not null
      UNION ALL
      SELECT user_id,timestamp FROM ios.removewatchlist where user_id is not null ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
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

  measure: removewatchlist_count {
    type: count
    drill_fields: []
  }
}
