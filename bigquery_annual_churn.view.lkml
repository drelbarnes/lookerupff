view: bigquery_annual_churn {
  derived_table: {
    sql:  with a0 as
      (select user_id,
              max((status_date)) as max_date
      from http_api.purchase_event
      group by 1),

      a1 as
      (select a01.user_id,
              a01.topic,
              date(max_date) as max_date
      from a0 inner join http_api.purchase_event as a01 on a0.user_id=a01.user_id and max_date=status_date),

      a12 as
      (select user_id,
              date(status_date) as status_date1,
              max(status_date) as max_date2
       from http_api.purchase_event
       group by 1,2
       order by 1),

      b as
      (select a11.user_id,
             date(created_at) as created_date,
             date(status_date) as status_date,
             a11.topic,
             subscription_frequency,
             subscription_status,
             plan,
             LAG(date(status_date)) OVER (PARTITION BY a11.user_id ORDER BY date(status_date) ASC) as prior_status_date,
             case when LAG(a11.topic) OVER (PARTITION BY a11.user_id ORDER BY date(status_date) ASC) in ('customer.product.renewed','customer.created','customer.product.created','customer.product.free_trial_created','customer.product.free_trial_converted') and date_diff(date(status_date),date(LAG(date(status_date)) OVER (PARTITION BY a11.user_id ORDER BY date(status_date) ASC)),day)>45 and
             LAG(plan) OVER (PARTITION BY a11.user_id ORDER BY date(status_date) ASC)='standard' then 1 else 0 end as sub_1,
             case when frequency='yearly' and a111.status='enabled' then 1 else 0 end as sub_2
      from http_api.purchase_event as a11 inner join a12 on a11.user_id=a12.user_id and a11.status_date=max_date2 left join a1 on a11.user_id=a1.user_id left join svod_titles.customer_frequency as a111 on a11.user_id=cast(a111.customer_id as string) and date(event_created_at)=date(status_date)),

      c as
      (select *,
             case when status_date<'2021-01-06' then case when sub_1=1 or sub_2=1 then 'yearly' else 'monthly' end
                  when status_date>='2021-01-06' and subscription_status='enabled' then subscription_frequency end as sub_plan
      from b
      order by user_id,prior_status_date),

      d as
      (select distinct user_id,
                       created_date,
                       status_date,
                       sub_plan
      from c
      where (plan<>'none' and topic not in('customer.created','customer.product.free_trial_created','customer.product.free_trial_expired')) and sub_plan='yearly'),

      e as
      (select distinct user_id,
       date(status_date) as status_date
from http_api.purchase_event
where topic in ('customer.product.cancelled','customer.product.disabled','customer.product.expired')),

e11 as
(select event_created_at,
       count(distinct customer_id) as annual_churn
from svod_titles.customer_frequency
where (frequency='yearly' and status in ('disabled','cancelled','expired'))
group by 1),

e12 as
(select date(status_date) as status_date,
       count(distinct user_id) as annual_churn
from http_api.purchase_event
where subscription_frequency='yearly' and subscription_status in ('disabled','cancelled','expired')
group by 1),

e1 as
(select case when status_date is not null then status_date else event_created_at end as event_created_at,
        case when e12.annual_churn is not null then e12.annual_churn else e11.annual_churn end as annual_churn
  from e11 full join e12 on status_date=event_created_at),

f as
(select d.*,
       case when e.status_date is not null then e.status_date else null end as churn_date,
       case when (e.status_date is not null or (frequency='yearly' and status in ('disabled','cancelled','expired'))) then 1 else 0 end as churn,
       case when d.status_date between date_add(e.status_date,interval 21 day) and e.status_date and (case when e.status_date is not null then 1 else 0 end)=0 then date_add(d.status_date,interval 1 year) else e.status_date end as one_year

from d left join e on d.user_id=e.user_id and (d.status_date<e.status_date and date_add(d.status_date,interval 395 day)>e.status_date)
       left join svod_titles.customer_frequency as f on d.user_id=cast(customer_id as string) and date(d.status_date)=date(event_created_at)),

f1 as
(select status_date,
       sum(churn) as churn,
       count(distinct user_id) as total_annual
from f left join e1 on status_date=event_created_at
group by 1)

select status_date,
       churn + annual_churn as churn,
       total_annual
from f1 left join e1 on status_date=event_created_at
order by 1 desc
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: status_date {
    type: date
    datatype: date
    sql: ${TABLE}.status_date ;;
  }

  dimension_group: status_date {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: timestamp(${TABLE}.status_date) ;;
  }

  dimension: churn {
    type: number
    sql: ${TABLE}.churn ;;
  }

  dimension: total_annual {
    type: number
    sql: ${TABLE}.total_annual ;;
  }

  measure: churn_ {
    type: sum
    sql: ${churn} ;;
  }

  measure: total_annual_ {
    type: sum
    sql: ${total_annual} ;;
  }

  set: detail {
    fields: [status_date, churn, total_annual]
  }
}
