view: bigquery_churn_cohorts {
  derived_table: {
    sql: with a as
      (select date(created_at) as created_at,
              count(distinct user_id) as created_count
      from http_api.purchase_event
      where date(created_at)>'2018-10-31' and topic<>'customer.created'
      group by 1),

      b as
      (select date(status_date) as status_date,
      created_at,
              user_id,
              status,
              charge_status,
              topic
      from http_api.purchase_event
      where date(created_at)>'2018-10-31' and ((topic in ('customer.product.expired','customer.product.disabled','customer.product.cancelled')
      or charge_status='expired'
      or status in ('customer.product.expired','cancelled','disabled'))
      or ((topic='customer.product.renewed' or status='renewed')))
      order by user_id,status_date),

      c as
      (select user_id,
            date(created_at) as created_at,
            status_date,
            case when (topic in ('customer.product.expired','customer.product.disabled','customer.product.cancelled')
or charge_status='expired'
or status in ('customer.product.expired','cancelled','disabled')) then "Churn"
           when ((topic='customer.product.renewed' or status='renewed'))
or (topic='customer.product.created') then "Renewed" end as status,
            rank() over (partition by user_id order by status_date) as billing_period
      from b)

      select c.created_at,
             billing_period,
             created_count,
             sum(case when status="Renewed" then 1 else 0 end) as Renewed,
             sum(case when status="Churn" then 1 else 0 end) as Churn
      from c inner join a on a.created_at=c.created_at
      group by 1,2,3
      order by 1,2,3
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: created_at {
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
    sql: ${TABLE}.created_at ;;}

  dimension: billing_period {
    type: number
    sql: ${TABLE}.billing_period ;;
  }

  dimension: created_count {
    type: number
    sql: ${TABLE}.created_count ;;
  }

  dimension: renewed {
    type: number
    sql: ${TABLE}.Renewed ;;
  }

  dimension: churn {
    type: number
    sql: ${TABLE}.Churn ;;
  }

  measure: created_count_ {
    type: sum
    sql: ${TABLE}.created_count ;;
  }

  measure: renewed_ {
    type: sum
    sql: ${TABLE}.Renewed ;;
  }

  measure: churn_ {
    type: sum
    sql: ${TABLE}.Churn ;;
  }

  set: detail {
    fields: [billing_period, created_count, renewed, churn]
  }
}
