view: bigquery_churn_cohorts {
  derived_table: {
    sql:with a as
      (select date(created_at) as created_at,
              platform,
              count(distinct user_id) as created_count
      from http_api.purchase_event
      where date(created_at)>'2018-10-31' and topic<>'customer.created' and plan='standard'
      group by 1,2),

      b as
      (select date(status_date) as status_date,
      created_at,
      platform,
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
            platform,
            status_date,
            case when (topic in ('customer.product.expired','customer.product.disabled','customer.product.cancelled')
or charge_status='expired'
or status in ('customer.product.expired','cancelled','disabled')) then "Churn"
           when ((topic='customer.product.renewed' or status='renewed'))
or (topic='customer.product.created') then "Renewed" end as status,
            rank() over (partition by user_id order by status_date) as billing_period
      from b),

d0 as
(select user_id,
        created_at,
       status,
       platform,
       min(billing_period) as billing_period
from c
where status='Churn'
group by 1,2,3,4),

d as
(select c.*
from c inner join d0 on c.user_id=d0.user_id and c.status=d0.status and c.billing_period=d0.billing_period),

e as
(select *
from c
where status='Renewed'),

f as
((select * from d)
union all
(select * from e))

      select f.created_at,
             billing_period,
             created_count,
             f.platform,
             sum(case when status="Renewed" then 1 else 0 end) as Renewed,
             sum(case when status="Churn" then 1 else 0 end) as Churn
      from f inner join a on a.created_at=f.created_at
      group by 1,2,3,4
      order by 1,2,3,4;;
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
    sql: timestamp(${TABLE}.created_at) ;;}

  dimension: billing_period {
    type: number
    sql: ${TABLE}.billing_period ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
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
