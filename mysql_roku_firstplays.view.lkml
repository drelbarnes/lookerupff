view: mysql_roku_firstplays {
  sql_table_name: admin_roku.firstplays ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: collection_id {
    type: number
    sql: ${TABLE}.collectionId ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: finishes {
    type: number
    sql: ${TABLE}.finishes ;;
  }

  dimension_group: firstplay_date {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.firstplay_date ;;
  }

  dimension: firstplays {
    type: number
    sql: ${TABLE}.firstplays ;;
  }

  dimension_group: ingestion {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.ingestion_date ;;
  }

  dimension: total_minutes_watched {
    type: number
    sql: ${TABLE}.total_minutes_watched ;;
  }

  dimension: upff {
    type: string
    sql: ${TABLE}.UPFF ;;
  }

  dimension: user_id {
    type: number
    tags:["user_id"]
    sql: ${TABLE}.userId ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.videoId ;;
  }

  measure: count_firstplays {
    type: count
    drill_fields: [id]
  }

  measure: count_subscribers {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: count_plays {
    type: sum
    sql: ${firstplays};;
  }

}
