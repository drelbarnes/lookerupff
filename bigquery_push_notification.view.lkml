view: bigquery_push_notification {
  derived_table: {
    sql: with mobile as
((select distinct b.user_id,
                context_device_advertising_id
from ios.users as a inner join http_api.purchase_event as b on a.email=b.email)
union all
(select distinct context_traits_user_id as user_id,
                 context_device_advertising_id
 from android.users)),

a1 as
(select distinct *
 from mobile
 where user_id<>'0'),

a2 as
(SELECT distinct
              a.user_id,
              TIMESTAMP(TIMESTAMP_MICROS(event_timestamp)) AS timestamp,
              a1.user_id as user_id2,
              1 as push
      FROM `up-faith-and-family.analytics_164012552.events_*` as a LEFT join unnest(user_properties) as e left join a1 on device.advertising_id=context_device_advertising_id
      WHERE _TABLE_SUFFIX>= '20210101'  AND event_name = 'view'
AND device.operating_system in ('Android', 'iOS') AND e.key = 'firebase_last_notification'),

a3 as
(select distinct case when user_id is null then user_id2 else user_id end as user_id,
       timestamp,
       push
from a2)

select *
from a3
where user_id is not null
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

  dimension: timestamp {
    type: date
    datatype: date
    sql: ${TABLE}.timestamp ;;
  }


  dimension_group: timestamp_ {
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
    sql: timestamp(${TABLE}.timestamp) ;;}

  dimension: push {
    type: number
    sql: ${TABLE}.push ;;
  }

  measure: push_ {
    type: sum
    sql: ${push} ;;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  set: detail {
    fields: [user_id, timestamp,push]
  }
}
