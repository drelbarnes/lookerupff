view: bigquery_annual_subs {
  derived_table: {
    sql: (with a0 as
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
             plan,
             a11.email,
             LAG(date(status_date)) OVER (PARTITION BY a11.user_id ORDER BY date(status_date) ASC) as prior_status_date,
             case when LAG(a11.topic) OVER (PARTITION BY a11.user_id ORDER BY date(status_date) ASC) in ('customer.product.renewed','customer.created','customer.product.created','customer.product.free_trial_created') and date_diff(date(status_date),date(LAG(date(status_date)) OVER (PARTITION BY a11.user_id ORDER BY date(status_date) ASC)),day)>45 and
             LAG(plan) OVER (PARTITION BY a11.user_id ORDER BY date(status_date) ASC)='standard' then 1 else 0 end as sub_1,
              case when frequency='yearly' and a111.status='enabled' then 1 else 0 end as sub_2
      from http_api.purchase_event as a11 inner join a12 on a11.user_id=a12.user_id and a11.status_date=max_date2 left join a1 on a11.user_id=a1.user_id left join svod_titles.customer_frequency as a111 on a11.user_id=cast(a111.customer_id as string) and date(event_created_at)=date(status_date)
      order by user_id,
               status_date asc),

      c as
      (select *,
             case when sub_1=1 or sub_2=1 then 'yearly' else 'monthly' end as sub_plan
      from b
      order by user_id,prior_status_date)

      select *,
             case when lag(prior_status_date) over (partition by user_id order by date(status_date) asc) is null and prior_status_date is not null then 1 else 0 end as free_trial,
             case when LAG(sub_plan) OVER (PARTITION BY user_id ORDER BY date(status_date) ASC)='monthly' and sub_plan='yearly' and LAG(topic) OVER (PARTITION BY user_id ORDER BY date(status_date) asc) in ('customer.product.created','customer.product.renewed') and topic <>'customer.product.set_cancellation' then 1 else 0 end as annual_conversion,
             case when sub_plan='yearly' and LAG(topic) OVER (PARTITION BY user_id ORDER BY date(status_date) asc) in ('customer.created','customer.product.free_trial_created') then 1 else 0 end as free_to_annual
      from c
      where plan<>'none' or topic<>'customer.created'
       )
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

  dimension: created_date {
    type: date
    datatype: date
    sql: ${TABLE}.created_date ;;
  }

dimension_group: created_date_ {
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
    sql: timestamp(${TABLE}.created_date) ;;
  }

dimension: email {
  type: string
  sql: ${TABLE}.email ;;
}

  dimension: status_date {
    type: date
    datatype: date
    sql: ${TABLE}.status_date ;;
  }

  dimension_group: status_date_ {
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

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: plan {
    type: string
    sql: ${TABLE}.plan ;;
  }

  dimension: prior_status_date {
    type: date
    datatype: date
    sql: ${TABLE}.prior_status_date ;;
  }

  dimension_group: prior_status_date_ {
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
    sql: timestamp(${TABLE}.prior_status_date) ;;
  }

  dimension: free_trial {
    type: number
    sql: ${TABLE}.free_trial ;;
  }

  dimension: sub_1 {
    type: number
    sql: ${TABLE}.sub_1 ;;
  }

  dimension: sub_2 {
    type: number
    sql: ${TABLE}.sub_2 ;;
  }

  dimension: sub_plan {
    type: string
    sql: ${TABLE}.sub_plan ;;
  }

  dimension: annual_conversion {
    type: number
    sql: ${TABLE}.annual_conversion ;;
  }

  dimension: free_to_annual {
    type: number
    sql: ${TABLE}.free_to_annual ;;
  }

  measure: annual_conversion_ {
    type: sum
    sql: ${TABLE}.annual_conversion ;;
  }

  measure: free_to_annual_ {
    type: sum
    sql: ${free_to_annual} ;;
  }

  measure: monthly_subs {
    type: count_distinct
    sql: case when ${sub_plan}='monthly' then ${user_id} else null end ;;
  }

  measure: yearly_subs {
    type: count_distinct
    sql: case when ${sub_plan}='yearly' then ${user_id} else null end ;;
  }

  measure: free_trial_count {
    type: sum
    sql: ${free_trial} ;;
  }

  set: detail {
    fields: [
      user_id,
      created_date,
      status_date,
      topic,
      plan,
      prior_status_date,
      sub_1,
      sub_2,
      sub_plan,
      annual_conversion,
      free_to_annual,
      free_trial
    ]
  }
}
