view: vimeo_ott_subscriber_events {
  derived_table: {
    sql: with purchase_events as (
          select user_id, subscription_status, event, platform
          , case
            when subscription_frequency in (null, 'custom', 'monthly') then 'monthly'
            else 'yearly'
            end as subscription_frequency
          , "timestamp"
          from ${vimeo_ott_webhook_events.SQL_TABLE_NAME}
          where event != 'customer_created'
        )
        , max_events as (
        select user_id, subscription_status, event, platform, subscription_frequency, "timestamp", row_number() over (partition by user_id, platform, date("timestamp") order by timestamp desc) as rn
        from purchase_events
        )
        , distinct_events_p0 as (
        select user_id, subscription_status, event, platform, subscription_frequency, date("timestamp") as date
        from max_events
        where rn = 1
        )
        , distinct_events as (
        select *
        , LAG(event) OVER (PARTITION BY user_id, platform ORDER BY "date") AS previous_event
        from distinct_events_p0
        )
        , users as (
        select user_id, platform, min(date("timestamp")) as min_date, current_date as max_date from purchase_events group by user_id, platform
        )
        , dates as (
        select date("timestamp") as "date" from purchase_events
        group by 1
        )
        , exploded_dates_per_user as (
        SELECT a.user_id, a.platform, d."date"
        FROM users a
        JOIN dates d ON d."date" >= a.min_date
        AND d."date" <= a.max_date
        )
        , join_events as (
          select a."date", a.user_id, b.subscription_status, b.event, b.previous_event, b.platform, b.subscription_frequency
          from exploded_dates_per_user as a
          left join distinct_events as b
          on a.user_id = b.user_id and a."date" = b."date" and a.platform = b.platform
        )
        , customer_record_p0 as (
          select "date" as "timestamp"
          , user_id
          , max(subscription_status) over (partition by user_id, status_group) as subscription_status
          , max(event) over (partition by user_id, status_group) as event
          , max(previous_event) over (partition by user_id, status_group) as previous_event
          , max(platform) over (partition by user_id, status_group) as platform
          , max(subscription_frequency) over (partition by user_id, status_group) as subscription_frequency
          -- , sum(coalesce(status_group / nullif(status_group,0),1)) over (partition by user_id, status_group order by "date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as days_at_status
          -- , count(status_group) over (partition by user_id, status_group) as total_days_at_status
          -- , null as days_on_record
          from (
            select *, count(subscription_status) over (partition by user_id order by "date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as status_group from join_events
          )
        )
        , customer_record as (
          select
          "timestamp",
          user_id,
          subscription_status,
          event,
          previous_event,
          platform,
          subscription_frequency
          from customer_record_p0
          group by 1,2,3,4,5,6,7
        )
      select *, row_number() over (order by "timestamp") as row from customer_record where "timestamp" is not null
       ;;
      datagroup_trigger: upff_acquisition_reporting
      distribution_style: all
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: previous_event {
    type: string
    sql: ${TABLE}.previous_event ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: row {
    type: number
    primary_key: yes
    sql: ${TABLE}.row ;;
  }

  set: detail {
    fields: [
      timestamp_date,
      user_id,
      subscription_status,
      event,
      platform,
      subscription_frequency,
      row
    ]
  }
}
