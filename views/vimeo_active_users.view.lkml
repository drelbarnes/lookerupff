view: vimeo_active_users {
  derived_table: {
    sql: with events as (
      select user_id, subscription_status as status, event as topic, platform, subscription_frequency as frequency, timestamp from ${vimeo_webhook_events.SQL_TABLE_NAME}
      )
      , max_events as (
      select user_id, status, topic, platform, frequency, timestamp, row_number() over (partition by user_id, extract(date from timestamp) order by timestamp desc) as rn
      from events
      )
      , distinct_events as (
      select user_id, status, topic, platform, frequency, extract(date from timestamp) as date
      from max_events
      where rn = 1
      )
      , users as (
       select user_id, min(extract(date from timestamp)) as min_date, current_date as max_date from events group by user_id
      )
      , dates as (
      select extract(date from timestamp) as date from events
      group by 1
      )
      , exploded_dates_per_user as (
      SELECT a.user_id, d.date
      FROM users a
      JOIN dates d ON d.date >= a.min_date
       AND d.date <= a.max_date
      )
      , join_events as (
      select a.date, a.user_id, b.status, b.topic, b.platform, b.frequency
      from exploded_dates_per_user as a
      left join distinct_events as b
      on a.user_id = b.user_id and a.date = b.date
      )
      -- select * from users order by min_date
      , fill_exploded_dates as (
        select date, user_id
        , max(status) over (partition by user_id, status_group) as status
        , max(topic) over (partition by user_id, status_group) as topic
        , max(platform) over (partition by user_id, status_group) as platform
        , max(frequency) over (partition by user_id, status_group) as frequency
        , count(status_group) over (partition by user_id, status_group) as days_at_status
        from (
          select *, count(status) over (partition by user_id order by date) as status_group from join_events
        )
      )
      , active_users as (
      select date, user_id, status, topic, platform, frequency from fill_exploded_dates where status in ("enabled", "free_trial") and days_at_status <= 365
      )
      , dedup as (
        select distinct * from active_users
      )
      select *, row_number() over (order by date, user_id) as row from dedup ;;

    datagroup_trigger: upff_daily_refresh_datagroup
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: row {
    primary_key: yes
    type: number
    sql: ${TABLE}.row ;;
  }

  dimension: date {
    type: date
    datatype: date
    sql: ${TABLE}.date ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
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

  set: detail {
    fields: [
      date,
      user_id,
      status,
      topic,
      platform,
      frequency,
      row
    ]
  }
}
