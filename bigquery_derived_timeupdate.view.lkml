view: bigquery_derived_timeupdate {

  derived_table: {
    sql: (with a as
        (select a.timestamp,
                user_id,
                b.platform,
                a.timecode,
                'Android' as source
         from android.timeupdate as a left join customers.subscribers as b
         on SAFE_CAST(a.user_id AS INT64) = b.customer_id
         union all
         select a.timestamp,
                user_id,
                b.platform,
                a.timecode,
                'iOS' as source
         from ios.timeupdate as a left join customers.subscribers as b
         on SAFE_CAST(a.user_id AS INT64) = b.customer_id
         union all
         select a.timestamp,
                user_id,
                b.platform,
                a.current_time AS timecode,
                'Web' as source
         from javascript.timeupdate as a left join customers.subscribers as b
         on SAFE_CAST(a.user_id AS INT64) = b.customer_id
        )

select a.*, status
from a inner join customers.subscribers on SAFE_CAST(user_id AS INT64) = customer_id) ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }


  dimension: timecode {
    type: number
    sql: ${TABLE}.timecode / 1000 ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }


  dimension_group: timestamp {
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

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: timecode_count {
    type: sum
    value_format: "0"
    sql: ${timecode} ;;
  }

# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }

}
