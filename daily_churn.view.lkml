view: daily_churn {
  derived_table: {
    sql: with

            a as (
            select * from http_api.purchase_event
            ),

            b as (
            select
              user_id,
              row_number() over (partition by user_id order by timestamp) as event_num,
              topic,
              platform,
              date(created_at) as create_dt,
              date(timestamp) as status_dt,
              datediff('day', date(created_at), date(timestamp)) as tenure,
              extract(dayofweek from date(created_at)) as day_of_week,
              case when topic in ('customer.product.free_trial_created') then 1 else 0 end as trialist
            from a
            where topic is not null
            order by user_id
            ),

            j as (
            select
              user_id,
              trialist
              from b
              group by 1,2
            ),

            k as (
            select user_id, trialist from j where trialist = 1
            ),

            c as (
            select
              user_id as uid,
              max(event_num) as max_event
            from b
            group by 1
            order by 1
            ),

            d as (
            select b.*, c.*
            from b inner join c
            on b.user_id = c.uid
            and b.event_num = c.max_event
            where user_id in (select user_id from k)
            order by user_id
            ),

            e as (
            select *,
              case
                when topic in ('customer.deleted', 'customer.product.free_trial_expired', 'customer.product.set_cancellation', 'customer.product.cancelled', 'customer.product.expired') then 1 else 0 end as churn_status,
              case
                when tenure between 0 and 30 then 1
                when tenure between 30 and 60 then 2
                when tenure between 60 and 90 then 3
                when tenure between 90 and 120 then 4
                when tenure between 120 and 150 then 5
                when tenure between 150 and 180 then 6
                when tenure between 180 and 210 then 7
                when tenure between 210 and 240 then 8
                when tenure between 240 and 270 then 9
                when tenure between 270 and 300 then 10
                when tenure between 300 and 330 then 11
                when tenure between 330 and 360 then 12
                else 13 end as num_months,
              case
                when day_of_week in (0,6) then 'weekend_sub'
                when day_of_week in (2,3) then 'weekday_sub'
                else 'other_sub' end as cust_type
            from d
            )

            select
              day_of_week,
              num_months,
              sum(churn_status) as num_churners,
              count(*) as total_pop,
              round(cast((num_churners*100.0)/(total_pop) as decimal(5,2)), 5) as rate
            from e
            where day_of_week is not null
            group by 1,2,3,4
            order by 1,2,3,4
             ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: day_of_week {
    type: number
    sql: ${TABLE}.day_of_week ;;
  }

  dimension: num_months {
    type: number
    sql: ${TABLE}.num_months ;;
  }

  measure: num_churners {
    type: number
    sql: ${TABLE}.num_churners ;;
  }

  measure: total_pop {
    type: number
    sql: ${TABLE}.total_pop ;;
  }

  measure: rate {
    type: number
    sql: ${TABLE}.rate ;;
  }

  set: detail {
    fields: [day_of_week, num_months, num_churners, total_pop, rate]
  }
}
