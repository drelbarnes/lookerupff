view: bigquery_churn_model_addwatchlist {
  derived_table: {
    sql:
  (SELECT user_id,timestamp, 1 as addwatchlist FROM javascript.addwatchlist where user_id is not null
  UNION ALL
  SELECT user_id,timestamp, 1 as addwatchlist FROM android.addwatchlist where user_id is not null
  UNION ALL
  SELECT user_id,timestamp, 1 as addwatchlist FROM ios.addwatchlist where user_id is not null);;
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

  measure: addwatchlist_count {
    type: sum
    sql: ${TABLE}.addwatchlist ;;
  }
  }
