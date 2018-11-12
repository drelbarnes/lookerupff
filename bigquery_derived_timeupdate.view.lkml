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

  dimension: hours_watched {
    type: number
    sql: ${timecode}/3600 ;;
    value_format: "0.00"
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

  measure: hours_count {
    type: sum
    value_format: "0.00"
    sql: ${hours_watched};;
  }


  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: hours_watched_per_user {
    type: number
    sql: 1.00*${hours_count}/${user_count} ;;
    value_format: "0.00"
  }

# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }

  parameter: date_granularity {
    type: string
    allowed_value: { value: "Day" }
    allowed_value: { value: "Week"}
    allowed_value: { value: "Month" }
    allowed_value: { value: "Quarter" }
    allowed_value: { value: "Year" }
  }

  dimension: date {
    label_from_parameter: date_granularity
    sql:
       CASE
         WHEN {% parameter date_granularity %} = 'Day' THEN
           ${timestamp_date}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Week' THEN
           ${timestamp_week}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Month' THEN
           ${timestamp_month}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Quarter' THEN
           ${timestamp_quarter}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Year' THEN
           ${timestamp_year}::VARCHAR
         ELSE
           NULL
       END ;;
  }

}
