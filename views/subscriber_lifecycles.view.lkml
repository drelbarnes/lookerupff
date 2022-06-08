view: sql_runner_query {
  derived_table: {
    sql: with order_completed as (
        with web as (
          select
          -- timestamp_trunc(timestamp, day) as event_date
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
            -- timestamp_trunc(timestamp, day) as event_date
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
            -- timestamp_trunc(a.timestamp, day) as event_date
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
            -- timestamp_trunc(timestamp, day) as event_date
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
            -- timestamp_trunc(a.timestamp, day) as event_date
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
            -- timestamp_trunc(timestamp, day) as event_date
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
            -- timestamp_trunc(a.timestamp, day) as event_date
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
        /* FOR USER LEVEL DEBUGGING */
        -- where user_id = '22959470'
        -- where user_id in ("14703282", "25876923", "7304204", "23427849", "6576290", "18470857", "27645149", "11564618", "5181035", "40022939", "26464967", "34344546", "27273808", "4631277", "5443958", "17114567", "24241288", "37501042", "16890861", "34170982", "19830769", "15646593", "26916365", "21392832", "37541931", "17226739", "13719394", "20393552", "5372106", "6309694", "29224084", "27383354", "35252164", "14517161", "6034129", "15324111", "5425677", "27593269", "12362429", "24326691", "16836307", "17743660", "27473911", "18322777", "14066488", "23334190", "26136639", "22195802", "16805971", "24826048", "15374314", "16838206", "18192168", "29562088", "12476646", "19960403", "28075278", "9378114", "41400060", "20689483", "35792242", "29265575", "15045313", "27594372", "26639393", "15492561", "5130686", "20331168", "22929999", "34624763", "23704875", "5812651", "6162777", "42064809", "31753360", "42897587", "13652176", "34269455", "40115509", "13548647", "34705590", "19850499", "12070516", "38891861", "26725567", "27501293", "30780653", "16937293", "21907230", "20689918", "27037436", "22763095", "17583689", "5261432", "35760116", "17845308", "27461353", "13047781", "26075260", "24633932", "37750834", "15638978", "38714855", "28536662", "26803265", "34363086", "5449482", "5861159", "21999172", "15238416", "6767842", "4014309", "30505216", "6706999", "13549696", "27087320", "25980353", "618230", "54800", "6291312", "25142023", "6382991", "16926023", "3475874", "5322180", "20679173", "27314570", "6659409", "34144316", "5157570", "17161903", "13977386", "42416397", "38968549", "24067394", "27137778", "19881588", "18782726", "43956333", "14004123", "42533639", "38713449", "10913190", "10612920", "21764640", "23329374", "5914254", "25911632", "17949738", "31031336", "34034951", "26691347", "14709704", "28521043", "11045538", "5487407", "33971847", "17872558", "19635340", "6608250", "27887938", "20178733", "27970974", "18926963", "16920821", "12791810", "24328540", "37425657", "22708030", "19289062", "12839845", "20134801", "10443695", "27065734", "26572340", "19218078", "28752546", "27644327", "4427833", "3808486", "16824886", "18467329", "21511227", "27074656", "5287619", "12896025", "26380135", "3798789", "24004601", "5073124", "18405390", "20890722", "21485697", "6002024", "29190812", "26326711", "19881410", "5193087", "28044288", "27800631")
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
