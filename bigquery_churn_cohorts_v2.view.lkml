view: bigquery_churn_cohorts_v2 {
  derived_table: {
    sql:with a as
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
or (topic='customer.product.created') then "Renewed" end as status
      from b),

d0 as
(select user_id,
        created_at,
       status,
       min(status_date) as billing_period
from c
where status='Churn'
group by 1,2,3),

d as
(select c.*
from c inner join d0 on c.user_id=d0.user_id and c.status=d0.status and c.status_date=d0.billing_period),

e as
(select *
from c
where status='Renewed'),

f as
((select * from d)
union all
(select * from e))

      select f.created_at,
             status_date as billing_period,
             created_count,
             sum(case when status="Renewed" then 1 else 0 end) as Renewed,
             sum(case when status="Churn" then 1 else 0 end) as Churn
      from f inner join a on a.created_at=f.created_at
      group by 1,2,3
      order by 1,2,3

       ;;
  }

  measure: count {
    type: count
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

  dimension_group: billing_period {
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
    sql: ${TABLE}.billing_period ;;}

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

}
