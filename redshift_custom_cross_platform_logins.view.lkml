view: redshift_custom_cross_platform_logins {
  derived_table: {
    sql: with web as (SELECT user_id, platform, timestamp FROM javascript.sign_in_complete),

      ios as (select CAST(user_id AS CHAR) as user_id, platform, timestamp platform FROM ios.sign_in_complete),

      android as (select user_id, platform, timestamp FROM android.sign_in_complete),

      fire_tv as (select user_id, platform, timestamp FROM amazon_fire_tv.sign_in_complete),

      roku as (select user_id, platform, timestamp FROM roku.sign_in_complete),

      j as

      (select * from web

      UNION ALL

      select * from ios

      UNION ALL

      select * from android

      UNION ALL

      select * from fire_tv

      UNION ALL

      select * from roku)

      select * from j

      /*q as (select count(user_id) as logins, platform, timestamp from j GROUP BY 2,3 )
      */

      /*select AVG(logins), platform FROM q GROUP BY 2*/
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: user_count {
    type: number
    sql: count(${user_id}) ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  measure: avg_logins {
    type: average
    sql:  ${user_id} ;;
  }

  set: detail {
    fields: [user_id, platform, timestamp_time]
  }
}
