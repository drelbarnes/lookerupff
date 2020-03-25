view: redshift_derived_mobile_app_engagement {

  derived_table: {

    sql:

       (select
          user_id,
          anonymous_id as anonymousId,
          received_at,
          'iOS' as platform,
          'app_opened' as event
        from ios.application_opened)

     UNION ALL

     (select
          user_id,
          anonymous_id as anonymousId,
          received_at,
          'iOS' as platform,
          'video_playing' as event
        from ios.video_content_playing)

     UNION ALL

    (select
          user_id,
          anonymous_id as anonymousId,
          received_at,
          'Android' as platform,
          'app_opened' as event
        from android.application_opened)

     UNION ALL

     (select
          user_id,
          anonymous_id as anonymousId,
          received_at,
          'Android' as platform,
          'video_playing' as event
        from android.video_content_playing);;

    }

  dimension: user_id {
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

    dimension: event {
      type: string
      sql: ${TABLE}.event ;;
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
      sql: ${anonymousId} ;;
    }

  measure: count_distinct_app_opened {
    type: count_distinct
    sql: ${anonymousId} ;;
    filters: {
      field: event
      value: "app_opened"
    }
  }

  measure: count_distinct_video_playing {
    type: count_distinct
    sql: ${anonymousId} ;;
    filters: {
      field: event
      value: "video_playing"
    }
  }

  measure: known_user_count_app_opened {
    label: "Known App Opened Users"
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: event
      value: "app_opened"
    }
  }

  measure: known_user_count_video_plays {
    label: "Known Video Plays Users"
    type: count_distinct
    sql: ${user_id} ;;
    filters: {
      field: event
      value: "video_playing"
    }
  }

}
