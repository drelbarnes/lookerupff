view: heartlandia {
  derived_table: {
    sql:
    select user_id, date(timestamp) as timestamp,
case when sum(high_price_churn)>0 then 1 else 0 end as high_price_churn,
case when sum(watched_heartland)>0 then 1 else 0 end as watched_heartland,
case when sum(IOS)>0 then 1 else 0 end as IOS,
case when sum(Android)>0 then 1 else 0 end as Android,
case when sum(Web)>0 then 1 else 0 end as Web
from (select distinct date(timestamp) as timestamp,user_id, high_price_churn, watched_heartland, IOS, Android, Web
    from ((select DATE(play.timestamp ) as timestamp, user_id,
    case when user_id||date(timestamp) in (select cast(userid as varchar)||date(occurred_at) from
                          customers.churn_custom_reasons
                           where occurred_at >= (TIMESTAMP '2018-04-10') AND (occurred_at) < (TIMESTAMP '2018-05-23')) then 1 else 0 end as high_price_churn,
    case when video_id in (select 231517 as video_id union all
select 245040 as video_id union all
select 231501 as video_id union all
select 231516 as video_id union all
select 240165 as video_id union all
select 231518 as video_id union all
select 245039 as video_id union all
select 214136 as video_id union all
select 209367 as video_id union all
select 209368 as video_id union all
select 209371 as video_id union all
select 212529 as video_id union all
select 214137 as video_id union all
select 212512 as video_id union all
select 214138 as video_id union all
select 209287 as video_id union all
select 209370 as video_id union all
select 212502 as video_id union all
select 212503 as video_id union all
select 212504 as video_id union all
select 224418 as video_id union all
select 212505 as video_id union all
select 212507 as video_id union all
select 212508 as video_id union all
select 209372 as video_id union all
select 209303 as video_id union all
select 209308 as video_id union all
select 213577 as video_id union all
select 213578 as video_id union all
select 209328 as video_id union all
select 209045 as video_id union all
select 209279 as video_id union all
select 212515 as video_id union all
select 209309 as video_id union all
select 214131 as video_id union all
select 212505 as video_id union all
select 209312 as video_id union all
select 209313 as video_id union all
select 209316 as video_id union all
select 209034 as video_id union all
select 209281 as video_id union all
select 209039 as video_id union all
select 209320 as video_id union all
select 212520 as video_id union all
select 209288 as video_id union all
select 209277 as video_id union all
select 212524 as video_id union all
select 208872 as video_id union all
select 209300 as video_id union all
select 208877 as video_id union all
select 209291 as video_id union all
select 208881 as video_id union all
select 209284 as video_id union all
select 209292 as video_id union all
select 209293 as video_id union all
select 209295 as video_id union all
select 212498 as video_id union all
select 209311 as video_id union all
select 209321 as video_id union all
select 209283 as video_id union all
select 209285 as video_id union all
select 208900 as video_id union all
select 213505 as video_id union all
select 209305 as video_id union all
select 213581 as video_id union all
select 208879 as video_id union all
select 212513 as video_id union all
select 212527 as video_id union all
select 209315 as video_id union all
select 208869 as video_id union all
select 208873 as video_id union all
select 212499 as video_id union all
select 209040 as video_id union all
select 208875 as video_id union all
select 212501 as video_id union all
select 212506 as video_id union all
select 212519 as video_id union all
select 209374 as video_id union all
select 209049 as video_id union all
select 212495 as video_id union all
select 213503 as video_id union all
select 209042 as video_id union all
select 213501 as video_id union all
select 208883 as video_id union all
select 209038 as video_id union all
select 213516 as video_id union all
select 209051 as video_id union all
select 212516 as video_id union all
select 224081 as video_id union all
select 240167 as video_id union all
select 224080 as video_id union all
select 240168 as video_id union all
select 245041 as video_id union all
select 231519 as video_id union all
select 254077 as video_id union all
select 240166 as video_id union all
select 254078 as video_id union all
select 214133 as video_id union all
select 214134 as video_id union all
select 224079 as video_id union all
select 245038 as video_id union all
select 209369 as video_id union all
select 212526 as video_id union all
select 209286 as video_id union all
select 214139 as video_id union all
select 209033 as video_id union all
select 209037 as video_id union all
select 209324 as video_id union all
select 209325 as video_id union all
select 209375 as video_id union all
select 209326 as video_id union all
select 209378 as video_id union all
select 214140 as video_id union all
select 209327 as video_id union all
select 212517 as video_id union all
select 209329 as video_id union all
select 212518 as video_id union all
select 208867 as video_id union all
select 208868 as video_id union all
select 212528 as video_id union all
select 209310 as video_id union all
select 209314 as video_id union all
select 209035 as video_id union all
select 209318 as video_id union all
select 208884 as video_id union all
select 209319 as video_id union all
select 209276 as video_id union all
select 209278 as video_id union all
select 209041 as video_id union all
select 209322 as video_id union all
select 209323 as video_id union all
select 214135 as video_id union all
select 209289 as video_id union all
select 209280 as video_id union all
select 208871 as video_id union all
select 209298 as video_id union all
select 212525 as video_id union all
select 209376 as video_id union all
select 212521 as video_id union all
select 209301 as video_id union all
select 212522 as video_id union all
select 208876 as video_id union all
select 212523 as video_id union all
select 208880 as video_id union all
select 209290 as video_id union all
select 208882 as video_id union all
select 209294 as video_id union all
select 212497 as video_id union all
select 209296 as video_id union all
select 209297 as video_id union all
select 209299 as video_id union all
select 209302 as video_id union all
select 212500 as video_id union all
select 209307 as video_id union all
select 209282 as video_id union all
select 209377 as video_id union all
select 208878 as video_id union all
select 213515 as video_id union all
select 209304 as video_id union all
select 209306 as video_id union all
select 213539 as video_id union all
select 209373 as video_id union all
select 212511 as video_id union all
select 212494 as video_id union all
select 209317 as video_id union all
select 212514 as video_id union all
select 209043 as video_id union all
select 208874 as video_id union all
select 212510 as video_id union all
select 208870 as video_id union all
select 209044 as video_id union all
select 209046 as video_id union all
select 212509 as video_id union all
select 209047 as video_id union all
select 209048 as video_id union all
select 209050 as video_id union all
select 212496 as video_id) then 1 else 0 end as watched_heartland, 1 as IOS, 0 as Android, 0 as Web
from ios.play)
union all
(select DATE(play.timestamp) as timestamp, user_id,
case when user_id ||date(timestamp) in (select cast(userid as varchar)||date(occurred_at) from
                          customers.churn_custom_reasons
                          where occurred_at >= (TIMESTAMP '2018-04-10') AND (occurred_at) < (TIMESTAMP '2018-05-23'))
                          then 1 else 0 end as high_price_churn,
case when video_id in (select 231517 as video_id union all
select 245040 as video_id union all
select 231501 as video_id union all
select 231516 as video_id union all
select 240165 as video_id union all
select 231518 as video_id union all
select 245039 as video_id union all
select 214136 as video_id union all
select 209367 as video_id union all
select 209368 as video_id union all
select 209371 as video_id union all
select 212529 as video_id union all
select 214137 as video_id union all
select 212512 as video_id union all
select 214138 as video_id union all
select 209287 as video_id union all
select 209370 as video_id union all
select 212502 as video_id union all
select 212503 as video_id union all
select 212504 as video_id union all
select 224418 as video_id union all
select 212505 as video_id union all
select 212507 as video_id union all
select 212508 as video_id union all
select 209372 as video_id union all
select 209303 as video_id union all
select 209308 as video_id union all
select 213577 as video_id union all
select 213578 as video_id union all
select 209328 as video_id union all
select 209045 as video_id union all
select 209279 as video_id union all
select 212515 as video_id union all
select 209309 as video_id union all
select 214131 as video_id union all
select 212505 as video_id union all
select 209312 as video_id union all
select 209313 as video_id union all
select 209316 as video_id union all
select 209034 as video_id union all
select 209281 as video_id union all
select 209039 as video_id union all
select 209320 as video_id union all
select 212520 as video_id union all
select 209288 as video_id union all
select 209277 as video_id union all
select 212524 as video_id union all
select 208872 as video_id union all
select 209300 as video_id union all
select 208877 as video_id union all
select 209291 as video_id union all
select 208881 as video_id union all
select 209284 as video_id union all
select 209292 as video_id union all
select 209293 as video_id union all
select 209295 as video_id union all
select 212498 as video_id union all
select 209311 as video_id union all
select 209321 as video_id union all
select 209283 as video_id union all
select 209285 as video_id union all
select 208900 as video_id union all
select 213505 as video_id union all
select 209305 as video_id union all
select 213581 as video_id union all
select 208879 as video_id union all
select 212513 as video_id union all
select 212527 as video_id union all
select 209315 as video_id union all
select 208869 as video_id union all
select 208873 as video_id union all
select 212499 as video_id union all
select 209040 as video_id union all
select 208875 as video_id union all
select 212501 as video_id union all
select 212506 as video_id union all
select 212519 as video_id union all
select 209374 as video_id union all
select 209049 as video_id union all
select 212495 as video_id union all
select 213503 as video_id union all
select 209042 as video_id union all
select 213501 as video_id union all
select 208883 as video_id union all
select 209038 as video_id union all
select 213516 as video_id union all
select 209051 as video_id union all
select 212516 as video_id union all
select 224081 as video_id union all
select 240167 as video_id union all
select 224080 as video_id union all
select 240168 as video_id union all
select 245041 as video_id union all
select 231519 as video_id union all
select 254077 as video_id union all
select 240166 as video_id union all
select 254078 as video_id union all
select 214133 as video_id union all
select 214134 as video_id union all
select 224079 as video_id union all
select 245038 as video_id union all
select 209369 as video_id union all
select 212526 as video_id union all
select 209286 as video_id union all
select 214139 as video_id union all
select 209033 as video_id union all
select 209037 as video_id union all
select 209324 as video_id union all
select 209325 as video_id union all
select 209375 as video_id union all
select 209326 as video_id union all
select 209378 as video_id union all
select 214140 as video_id union all
select 209327 as video_id union all
select 212517 as video_id union all
select 209329 as video_id union all
select 212518 as video_id union all
select 208867 as video_id union all
select 208868 as video_id union all
select 212528 as video_id union all
select 209310 as video_id union all
select 209314 as video_id union all
select 209035 as video_id union all
select 209318 as video_id union all
select 208884 as video_id union all
select 209319 as video_id union all
select 209276 as video_id union all
select 209278 as video_id union all
select 209041 as video_id union all
select 209322 as video_id union all
select 209323 as video_id union all
select 214135 as video_id union all
select 209289 as video_id union all
select 209280 as video_id union all
select 208871 as video_id union all
select 209298 as video_id union all
select 212525 as video_id union all
select 209376 as video_id union all
select 212521 as video_id union all
select 209301 as video_id union all
select 212522 as video_id union all
select 208876 as video_id union all
select 212523 as video_id union all
select 208880 as video_id union all
select 209290 as video_id union all
select 208882 as video_id union all
select 209294 as video_id union all
select 212497 as video_id union all
select 209296 as video_id union all
select 209297 as video_id union all
select 209299 as video_id union all
select 209302 as video_id union all
select 212500 as video_id union all
select 209307 as video_id union all
select 209282 as video_id union all
select 209377 as video_id union all
select 208878 as video_id union all
select 213515 as video_id union all
select 209304 as video_id union all
select 209306 as video_id union all
select 213539 as video_id union all
select 209373 as video_id union all
select 212511 as video_id union all
select 212494 as video_id union all
select 209317 as video_id union all
select 212514 as video_id union all
select 209043 as video_id union all
select 208874 as video_id union all
select 212510 as video_id union all
select 208870 as video_id union all
select 209044 as video_id union all
select 209046 as video_id union all
select 212509 as video_id union all
select 209047 as video_id union all
select 209048 as video_id union all
select 209050 as video_id union all
select 212496 as video_id) then 1 else 0 end as watched_heartland, 0 as IOS, 1 as Android, 0 as Web
from android.play)
union all
(select DATE(play.timestamp) as timestamp, user_id,
case when user_id||date(timestamp) in (select cast(userid as varchar)||date(occurred_at) from
                          customers.churn_custom_reasons
                          where occurred_at >= (TIMESTAMP '2018-04-10') AND (occurred_at) < (TIMESTAMP '2018-05-23') )
                          then 1 else 0 end as high_price_churn,
case when title ~* 'heartland'
then 1 else 0 end as watched_heartland, 0 as IOS, 0 as Android, 1 as Web
from javascript.play)))
group by user_id, timestamp
;;
  }

dimension: high_price_churn {
  type: number
  sql: ${TABLE}.high_price_churn ;;
}

measure: high_price_churn_total {
  type: sum
  sql: ${high_price_churn} ;;
}

  dimension_group: play_timestamp {
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
    sql: ${TABLE}.timestamp ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: watched_heartland {
    type: number
    sql: ${TABLE}.watched_heartland;;
  }

  dimension: IOS{
    type: number
    sql: ${TABLE}.IOS ;;
  }

  dimension: Android{
    type: number
    sql: ${TABLE}.Android ;;
  }

  dimension: Web{
    type: number
    sql: ${TABLE}.Web;;
  }

  measure: watched_heartland_total {
    type: sum
    sql: ${watched_heartland} ;;
  }

  measure: distinct_users {
    type: count_distinct
    sql: ${user_id} ;;
  }

  }
