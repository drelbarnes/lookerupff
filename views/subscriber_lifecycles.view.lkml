view: subscriber_lifecycles {
  derived_table: {
    sql: with order_completed as (
        with web as (
          select
          timestamp as event_date
          , user_id
          , anonymous_id
          , context_ip
          , context_traits_cross_domain_id
          , context_traits_email as email
          , context_user_agent
          , platform
          , event
          , "" as status
          , '' as charge_status
          , "" as frequency
          , revenue
          FROM javascript.order_completed
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13
          )
        , roku as (
          with existing as (
            select
            timestamp as event_date
            , user_id
            , anonymous_id
            , '' as context_ip
            , '' as context_traits_cross_domain_id
            , context_traits_email as email
            , context_user_agent
            , platform
            , event
            , '' as status
            , '' as charge_status
            , '' as frequency
            , null as revenue
            FROM roku.order_completed
            where user_id not in ('0', null)
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13
          )
          , new_users as (
            select
            a.timestamp as event_date
            , b.user_id
            , a.anonymous_id
            , '' as context_ip
            , '' as context_traits_cross_domain_id
            , a.context_traits_email as email
            , a.context_user_agent
            , a.platform
            , a.event
            , '' as status
            , '' as charge_status
            , '' as frequency
            , null as revenue
            from roku.order_completed as a
            inner join roku.account_created as b
            on a.session_id = b.session_id
            where b.user_id not in ('0', null)
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13
          )
          select * from existing
          union all
          select * from new_users
        )
        , ios as (
          with existing as (
            select
            timestamp as event_date
            , cast(user_id as string) as user_id
            , anonymous_id
            , '' as context_ip
            , '' as context_traits_cross_domain_id
            , context_traits_email as email
            , context_user_agent
            , platform
            , event
            , '' as status
            , '' as charge_status
            , '' as frequency
            , null as revenue
            FROM ios.order_completed
            where cast(user_id as string) != '0' and user_id is not null
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13
          )
          , new_users as (
            select
            a.timestamp as event_date
            , cast(b.user_id as string) as user_id
            , a.anonymous_id
            , '' as context_ip
            , '' as context_traits_cross_domain_id
            , a.context_traits_email as email
            , a.context_user_agent
            , a.platform
            , a.event
            , '' as status
            , '' as charge_status
            , '' as frequency
            , null as revenue
            from ios.order_completed as a
            inner join ios.account_created as b
            on a.session_id = b.session_id
            where cast(b.user_id as string) != '0' and b.user_id is not null
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13
          )
          select * from existing
          union all
          select * from new_users
        )
        , android as (
          with existing as (
            select
            timestamp as event_date
            , cast(user_id as string) as user_id
            , anonymous_id
            , '' as context_ip
            , '' as context_traits_cross_domain_id
            , context_traits_email as email
            , context_user_agent
            , platform
            , event
            , '' as status
            , '' as charge_status
            , '' as frequency
            , null as revenue
            FROM android.order_completed
            where cast(user_id as string) != '0' and user_id is not null
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13
          )
          , new_users as (
            select
            a.timestamp as event_date
            , cast(b.user_id as string) as user_id
            , a.anonymous_id
            , '' as context_ip
            , '' as context_traits_cross_domain_id
            , a.context_traits_email as email
            , a.context_user_agent
            , a.platform
            , a.event
            , '' as status
            , '' as charge_status
            , '' as frequency
            , null as revenue
            from android.order_completed as a
            inner join android.account_created as b
            on a.session_id = b.session_id
            where cast(b.user_id as string) != '0' and b.user_id is not null
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13
          )
          select * from existing
          union all
          select * from new_users
        )
        , all_platforms as (
          select * from web
          union all
          select * from roku
          union all
          select * from ios
          union all
          select * from android
        )
        select *
        , row_number() over (partition by user_id order by event_date) as lifecycle
        from all_platforms
      )
      , purchase_events as (
      select
      timestamp as event_date
      , user_id
      , "" as anonymous_id
      , "" as context_ip
      , "" as context_traits_cross_domain_id
      , email as context_traits_email
      , "" context_user_agent
      , platform
      , topic as event
      , subscription_status as status
      , charge_status
      , subscription_frequency as frequency
      , case
          when topic in ("customer.product.created", "customer.product.free_trial_converted", "customer.product.renewed") then subscription_price/100
          else 0
        end as revenue
      , null as lifecycle
      from http_api.purchase_event
      where platform in ("web", "roku", "ios", "tvos", "android", "android_tv")
      and topic not in ('customer.created', 'customer.updated', 'customer.product.charge_failed')
      )
      , all_events as (
        with events as (
          select * from order_completed
          union all
          select * from purchase_events
          )
        select * from events
        where user_id is not null
      )
      , partitioning as (
      select *
        , sum(case when lifecycle is null then 0 else 1 end) over (order by user_id, event_date) as lifecycle_partition
        from all_events
        order by event_date
      )
      , lifecycles as (
      select
      event_date
      , user_id
      , first_value(lifecycle) over (partition by lifecycle_partition order by user_id, event_date) as lifecycle
      , event
      , status
      , charge_status
      , frequency
      , anonymous_id
      , context_ip
      , context_traits_cross_domain_id
      , email
      , context_user_agent
      , platform
      , revenue
      from partitioning
      )
      , agg as (
      select user_id
      , max(lifecycle) as lifecycles
      , round(sum(revenue), 2) as ltv
      from lifecycles
      group by user_id
      )
      -- work in progress
      , summary as (
        with rn as (
          select *
          , row_number() over (partition by user_id, lifecycle order by event_date) as row
          from lifecycles
        )
        -- create table of first records for each lifecyle per user
        , start as (
          select user_id
          , lifecycle
          , event_date as start_date
          , platform
          from rn
          where row = 1
        )
        -- create table of last records for each lifecyle per user
        , last as (
          select user_id
          , lifecycle
          , max(event_date) as last_status_date
          from lifecycles
          group by 1,2
        )
        select
        -- join tables into one record with last known status and frequency
        last.user_id
        , last.lifecycle
        , start.platform
        , start.start_date
        , last.last_status_date
        , lifecycles.status as last_known_status
        , lifecycles.charge_status as last_charge_status
        , lifecycles.frequency
        from last
        left join start
        on last.user_id = start.user_id and last.lifecycle = start.lifecycle
        left join lifecycles
        on last.user_id = lifecycles.user_id and last.last_status_date = lifecycles.event_date
      )
      select
      user_id
      , lifecycle
      , platform
      , frequency
      , start_date
      , last_status_date
      , last_known_status
      , last_charge_status
      from summary
       ;;

    persist_for: "6 hours"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: lifecycle {
    type: number
    sql: ${TABLE}.lifecycle ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension_group: start_date {
    type: time
    sql: ${TABLE}.start_date ;;
  }

  dimension_group: last_status_date {
    type: time
    sql: ${TABLE}.last_status_date ;;
  }

  dimension: last_known_status {
    type: string
    sql: ${TABLE}.last_known_status ;;
  }

  dimension: last_charge_status {
    type: string
    sql: ${TABLE}.last_charge_status ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  set: detail {
    fields: [
      user_id,
      lifecycle,
      platform,
      start_date_time,
      last_status_date_time,
      last_known_status,
      last_charge_status,
      frequency
    ]
  }
}
