view: customer_frequency {
  derived_table: {
    sql: select  date(status_date) as status_date,
                 count(distinct case when subscription_frequency='yearly' then user_id end) as annual_signups,
                 count(distinct case when subscription_frequency='monthly' then user_id end) as monthly_signups
from http_api.purchase_event
where topic in ('customer.created','customer.product.free_trial_created')
group by 1
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group:status_date {
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
    sql: timestamp(${TABLE}.status_date) ;;
  }

  dimension: annual_signups {
    type: number
    sql: ${TABLE}.annual_signups ;;
  }

  measure: annual_signups_ {
    type: sum
    sql: ${annual_signups} ;;
  }

  dimension: monthly_signups {
    type: number
    sql: ${TABLE}.monthly_signups ;;
  }

  measure: monthly_signups_ {
    type: sum
    sql: ${monthly_signups} ;;
  }


  set: detail {
    fields: [ annual_signups, monthly_signups]
  }
}
