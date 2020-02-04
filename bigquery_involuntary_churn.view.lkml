view: bigquery_involuntary_churn {
  derived_table: {
    sql: with a as
      (select distinct user_id,
             email,
             topic,
             fname,
             max(status_date) as status_date
      from http_api.purchase_event
      group by 1,2,3,4)

      select *
      from a
      where topic in ('customer.product.expired','customer.product.disabled')
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    tags: ["email"]
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: fname {
    type: string
    sql: ${TABLE}.fname ;;
  }

  dimension_group: status_date {
    type: time
    sql: ${TABLE}.status_date ;;
  }

  dimension: status {
    type: string
    sql: case
             when ${topic} = 'customer.product.expired' then "involuntary_churn"
             when ${topic} = 'customer.product.disabled' then "involuntary_churn" end;;
  }



  dimension: date_formatted {
    sql: ${status_date_date} ;;
    html: {{ rendered_value | date: "%m/%d/%Y" }};;
  }

  set: detail {
    fields: [user_id, email, topic, status_date_time]
  }
}
