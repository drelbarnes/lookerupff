view: customer_record {
  derived_table: {
    sql: with upff_events as (
        select customer_id, subscription_id, user_id, plan, subscription_status as status, event as topic, platform
        , case
          when subscription_frequency in (null, "custom", "monthly") then "monthly"
          else "yearly"
          end as frequency
        , timestamp
        from ${upff_webhook_events.SQL_TABLE_NAME}
        where event not in ("customer_created", "customer_updated")
      )
      , gtv_events as (
        select customer_id, subscription_id, user_id, plan, subscription_status as status, event as topic, platform
        , case
          when subscription_frequency in (null, "custom", "monthly") then "monthly"
          else "yearly"
          end as frequency
        , timestamp
        from ${gtv_webhook_events.SQL_TABLE_NAME}
        where event not in ("customer_created", "customer_updated")
      )
      , minno_events as (
        select customer_id, subscription_id, user_id, plan, subscription_status as status, event as topic, platform
        , case
          when subscription_frequency in (null, "custom", "monthly") then "monthly"
          else "yearly"
          end as frequency
        , timestamp
        from ${minno_webhook_events.SQL_TABLE_NAME}
        where event not in ("customer_created", "customer_updated")
      )
      , events as (
      select * from upff_events
      union all
      select * from gtv_events
      union all
      select * from minno_events
      )
      , max_events as (
      select customer_id, subscription_id, user_id, plan, status, topic, platform, frequency, timestamp, row_number() over (partition by subscription_id, extract(date from timestamp) order by timestamp desc) as rn
      from events
      )
      , distinct_events as (
      select customer_id, subscription_id, user_id, plan, status, topic, platform, frequency, extract(date from timestamp) as date
      from max_events
      where rn = 1
      )
      , users as (
      select customer_id, subscription_id, user_id, min(extract(date from timestamp)) as min_date, current_date as max_date from events group by customer_id, subscription_id, user_id
      )
      , dates as (
      select extract(date from timestamp) as date from events
      group by 1
      )
      , exploded_dates_per_user as (
      SELECT a.customer_id, a.subscription_id, a.user_id, d.date
      FROM users a
      JOIN dates d ON d.date >= a.min_date
      AND d.date <= a.max_date
      )
      , join_events as (
        select a.date, a.customer_id, a.subscription_id, a.user_id, b.plan, b.status, b.topic, b.platform, b.frequency
        from exploded_dates_per_user as a
        left join distinct_events as b
        on a.customer_id = b.customer_id and a.subscription_id = b.subscription_id and a.date = b.date
      )
      , customer_record as (
        select timestamp(date) as date, customer_id, subscription_id, user_id
        , max(plan) over (partition by subscription_id, status_group) as plan
        , max(status) over (partition by subscription_id, status_group) as status
        , max(topic) over (partition by subscription_id, status_group) as topic
        , max(platform) over (partition by subscription_id, status_group) as platform
        , max(frequency) over (partition by subscription_id, status_group) as frequency
        , sum(ifnull(status_group / nullif(status_group,0),1)) over (partition by subscription_id, status_group order by date) as days_at_status
        , count(status_group) over (partition by subscription_id, status_group) as total_days_at_status
        , date_diff(date, min(date) over (partition by customer_id), DAY) + 1 as days_on_record
        , date_diff(max(date) over (partition by customer_id), min(date) over (partition by customer_id), DAY) + 1 as total_days_on_record
        from (
          select *, count(status) over (partition by customer_id, subscription_id order by date) as status_group from join_events
        )
      )
      select *, row_number() over (order by date) as row from customer_record where date is not null
       ;;
      datagroup_trigger: upff_daily_refresh_datagroup
    }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: date {
    type: time
    sql: ${TABLE}.date ;;
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: plan {
    type: string
    sql: ${TABLE}.plan ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: days_at_status {
    type: number
    sql: ${TABLE}.days_at_status ;;
  }

  dimension: total_days_at_status {
    type: number
    sql: ${TABLE}.total_days_at_status ;;
  }

  dimension: days_on_record {
    type: number
    sql: ${TABLE}.days_on_record ;;
  }

  dimension: total_days_on_record {
    type: number
    sql: ${TABLE}.total_days_on_record ;;
  }

  dimension: row {
    type: number
    primary_key: yes
    sql: ${TABLE}.row ;;
  }

  set: detail {
    fields: [
      date_time,
      user_id,
      status,
      topic,
      platform,
      frequency,
      days_at_status,
      total_days_at_status,
      days_on_record,
      total_days_on_record,
      row
    ]
  }
}
