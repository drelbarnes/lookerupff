view: bigquery_14_day_churn_list {
  derived_table: {
    sql: with a1 as
      (select distinct user_id,
             email,
             status_date
      from http_api.purchase_event
      where topic = 'customer.product.charge_failed' and date(status_date)>=date_sub(current_date(),interval 14 day))

      select distinct a.user_id,
             a.email,
             topic,
             date(a.status_date) as status_date
      from http_api.purchase_event as a inner join a1 on a.user_id=a1.user_id and a.status_date>=a1.status_date
      where date(a.status_date)>=date_sub(current_date(),interval 14 day) and topic in ('customer.product.cancelled','customer.product.disabled','customer.product.expired')
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: status_date {
    type: date
    sql: ${TABLE}.status_date ;;
  }

  set: detail {
    fields: [user_id, email, topic, status_date]
  }
}
