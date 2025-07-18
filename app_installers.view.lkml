explore: app_installers {}
view: app_installers {
  derived_table: {
    sql:
      SELECT anonymous_id
      ,CASE
        WHEN context_device_model like '%TV%' THEN 'Android TV'
        ELSE 'Android'
      END AS platform
      ,timestamp FROM android.app_installed

      UNION ALL
        SELECT anonymous_id,
        CASE
          WHEN device = 'appletv' THEN 'tvOS'
          ELSE 'IOS'
        END AS platform
        ,timestamp FROM ios.app_installed

      UNION ALL
      SELECT
        anonymous_id
        ,CASE
          WHEN device = 'fire_tablet' THEN 'Amazon Fire Tablet'
          ELSE 'amazon_fire_tv'
        END AS platform
        ,timestamp
      FROM amazon_fire_tv.app_installed

      UNION ALL
      SELECT
        anonymous_id
        ,'Roku' as platform
        ,timestamp
      FROM roku.app_installed


       ;;
  }

  measure: count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: platform {
    type:  string
    sql: ${TABLE}.platform ;;
  }
  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

# Added for AGM Tues-Mon weekly reporting
  dimension_group: week_start_tuesday {
    type: time
    timeframes: [raw, date, day_of_week, week, month]
    sql: CASE
      WHEN ${timestamp_day_of_week} = 'Tuesday' THEN ${timestamp_date}
      WHEN ${timestamp_day_of_week} = 'Wednesday' THEN dateadd(days, -1, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Thursday' THEN dateadd(days, -2, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Friday' THEN dateadd(days, -3,  ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Saturday' THEN dateadd(days, -4, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Sunday' THEN dateadd(days, -5, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Monday' THEN dateadd(days, -6, ${timestamp_date})
      END;;
    datatype: date
  }

  ## filter determining time range for all "A" measures
  filter: timeframe_a {
    type: date_time
  }

  ## flag for "A" measures to only include appropriate time range
  dimension: group_a_yesno {
    hidden: yes
    type: yesno
    sql: {% condition timeframe_a %} ${timestamp_raw} {% endcondition %} ;;
  }

  ## filter determining time range for all "B" measures
  filter: timeframe_b {
    type: date_time
  }

  ## flag for "B" measures to only include appropriate time range
  dimension: group_b_yesno {
    hidden: yes
    type: yesno
    sql: {% condition timeframe_b %} ${timestamp_raw} {% endcondition %} ;;
  }

  measure: installs_a {
    type: count_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.anonymous_id ;;
    filters: [group_a_yesno: "yes"]
  }


  measure: installs_b {
    type: count_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.anonymous_id ;;
    filters: [group_b_yesno: "yes"]
  }

  set: detail {
    fields: [anonymous_id, timestamp_time]
  }
}
