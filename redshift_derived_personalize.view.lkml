view: redshift_derived_personalize {

  derived_table: {

    sql:

       (select
          user_id as USER_ID,
          CAST(video_id AS bigint) as ITEM_ID,
          anonymous_id as anonymousId,
          event as EVENT_TYPE,
          received_at,
          'iOS' as platform,
          CAST(date_part(epoch,timestamp) AS bigint) as TIMESTAMP
        from ios.firstplay WHERE date(timestamp)>='2019-02-01')

     UNION ALL


      (select
          user_id as USER_ID,
          video_id as ITEM_ID,
          anonymous_id as anonymousId,
          event as EVENT_TYPE,
          received_at,
          'android' as platform,
          CAST(date_part(epoch,timestamp) AS bigint) as TIMESTAMP
        from android.firstplay WHERE date(timestamp)>='2019-02-01')


      UNION ALL

       (select
         user_id as USER_ID,
         video_id as ITEM_ID,
         anonymous_id as anonymousId,
         event as EVENT_TYPE,
         received_at,
        'web' as platform,
        CAST(date_part(epoch,timestamp) AS bigint) as TIMESTAMP
       from javascript.firstplay WHERE date(timestamp)>='2019-02-01');;

  }

  dimension: userId {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: anonymousId {
    #tags: ["segment_anonymous_id"]
    type: string
    sql: ${TABLE}.anonymousId ;;
  }


  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: itemId {
    type: number
    value_format: "0"
    sql: ${TABLE}.item_id ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event_type;;
  }

  dimension: timestamp {
    type: number
    value_format: "0"
    sql: ${TABLE}.timestamp ;;
  }

  dimension_group: received {
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
    sql: ${TABLE}.received_at ;;
  }

  measure: count {
    type: count
  }

  measure: count_distinct {
    type: count_distinct
    sql: ${userId} ;;
  }

}
