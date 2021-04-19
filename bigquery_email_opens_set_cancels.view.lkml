view: bigquery_email_opens_set_cancels {
  derived_table: {
    sql: with a1 as
      (select a.email,
             (mysql_email_campaigns_timestamp_date) as opens_timestamp
      from looker.get_mailchimp_campaigns as a inner join http_api.purchase_event as b on a.email=b.email
      where mysql_email_campaigns_action='open'),

      a2 as
      (select opens_timestamp,
             count(distinct email) as opens_count
      from a1
      group by 1),

      a3 as
      (select a1.email,
             opens_timestamp
      from a1 inner join http_api.purchase_event as b on a1.email=b.email and date_diff(date(status_date),date(opens_timestamp),day)<=3
      where topic='customer.product.set_cancellation'),

      a4 as
      (select opens_timestamp,
             count(distinct email) as set_cancels
      from a3
      group by 1)

      select a2.opens_timestamp,
             (FORMAT_TIMESTAMP('%F', TIMESTAMP_TRUNC(a2.opens_timestamp , WEEK(MONDAY)))) as week,
             opens_count,
             set_cancels
      from a2 inner join a4 on a2.opens_timestamp=a4.opens_timestamp
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: week {
    type: date
    sql: ${TABLE}.week ;;
  }

  dimension_group: opens_timestamp {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      day_of_week_index,
      day_of_week,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.opens_timestamp ;;
  }



  dimension: opens_count {
    type: number
    sql: ${TABLE}.opens_count ;;
  }

  dimension: set_cancels {
    type: number
    sql: ${TABLE}.set_cancels ;;
  }

  measure: opens_count_ {
    type: sum
    sql: ${TABLE}.opens_count ;;
  }

  measure: set_cancels_ {
    type: sum
    sql: ${TABLE}.set_cancels ;;
  }

  set: detail {
    fields: [opens_timestamp_time,week, opens_count, set_cancels]
  }
}
