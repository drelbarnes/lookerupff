view: redshift_derived_personalize {

  derived_table: {

    sql:
       (select
       CAST(user_id AS bigint) as USER_ID,
       CAST(video_id AS bigint) as ITEM_ID,
       event as EVENT_TYPE,
       date_part(epoch,timestamp) as TIMESTAMP
     from ios.timeupdate WHERE date(timestamp)>='2019-02-01')

     UNION ALL

       (select
          CAST(user_id AS bigint) as USER_ID,
       CAST(video_id AS bigint) as ITEM_ID,
          event as EVENT_TYPE,
          date_part(epoch, timestamp) as TIMESTAMP
        from ios.firstplay WHERE date(timestamp)>='2019-02-01')

     UNION ALL

       (select
         user_id as USER_ID,
         video_id as ITEM_ID,
         event as EVENT_TYPE,
         date_part(epoch,timestamp) as TIMESTAMP
       from ios.view WHERE date(timestamp)>='2019-02-01')
    ;;

  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: item_id {
    type: number
    sql: ${TABLE}.item_id ;;
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type;;
  }

  dimension: timestamp {
    type: number
    value_format: "0"
    sql: ${TABLE}.timestamp ;;
  }

  measure: count {
    type: count
  }

}
