view: minno_schema {
  derived_table: {
    sql:
      SELECT
      subscriber_account_id
      ,updated_at
      ,CASE
        WHEN status = 'Trialing' THEN 'in_trial'
        ELSE status
      END AS status
      FROM customers.minno_customers;;
 }

  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.updated_at ;;
  }

  dimension: user_id {
    type: string
    sql:  ${TABLE}.subscriber_account_id ;;
  }

  dimension: status {
    type:  string
    sql: ${TABLE}.status ;;
  }
  measure: total_free_trials {
    type: count_distinct
    filters: [status: "in_trial"]
    sql: ${TABLE}.subscriber_account_id  ;;
  }
}
