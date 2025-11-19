view: redshift_php_get_churn_survey {
  sql_table_name: php.get_churn_survey ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: comment {
    type: string
    sql: ${TABLE}.comment ;;
  }

  dimension: context_library_consumer {
    type: string
    sql: ${TABLE}.context_library_consumer ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension_group: ingest {
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
    sql: ${TABLE}.ingest_date ;;
  }

  dimension_group: original_timestamp {
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
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension: other {
    type: string
    sql: ${TABLE}.other ;;
  }

  dimension: programming {
    type: string
    sql: ${TABLE}.programming ;;
  }


  dimension: programming_element_1 {
    type: string
    sql: SPLIT_PART(${programming}, ',' , 2);;
  }

  dimension: programming_array {
    type: string
    sql: replace(${programming_element_1}, ']', '');;
  }

  dimension: rating {
    type: string
    sql: ${TABLE}.rating ;;
    value_format: "0.0"
  }

  dimension: total_score {
    type: number
    label: "Score"
    value_format: "0"
    sql:  CAST(${TABLE}.rating AS INT) ;;
  }

  dimension: reason {
    type: string
    sql: ${TABLE}.reason ;;
  }

  dimension: reason_category {
    type: string
    case: {
      when: {
        sql: ${reason} ILIKE '%No time to watch%' ;;
        label: "no_time_to_watch"
      }
      when: {
        sql: ${reason} ILIKE '%Price%' OR
          ${reason} ILIKE '%The price is too high%' ;;
        label: "price"
      }
      when: {
        sql: ${reason} ILIKE '%Content Not Interesting%' OR
          ${reason} ILIKE '%There was not enough that I wanted to watch%' ;;
        label: "content_not_interesting"
      }
      when: {
        sql: ${reason} ILIKE '%Content Not Meeting Expectations%' OR
          ${reason} ILIKE '%The quality of content did not meet my expectations%' ;;
        label: "content_not_meeting_expectations"
      }
      when: {
        sql: ${reason} ILIKE '%There were too many technical/ functionality issues%' or ${reason} ILIKE '%Streaming Issues%' ;;
        label: "too_many_technical_issues"
      }
      when: {
        sql: ${reason} ILIKE '%I did not use it enough to continue the membership%' ;;
        label: "not_enough_usage"
      }

      when: {
        sql: ${reason} ILIKE '%on%' ;;
        label: "on"
      }
    }
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

  dimension: restart {
    type: string
    sql: ${TABLE}.restart ;;
  }

  dimension: total_restart {
    type: number
    label: "Restart Score"
    value_format: "0"
    sql:  CAST(${TABLE}.restart AS INT) ;;
  }

  measure: avg_restart {
    type: average
    sql: cast(${TABLE}.restart as INT) ;;
  }

  dimension_group: sent {
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
    sql: ${TABLE}.sent_at ;;
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

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    tags: ["user_id"]
  }

  dimension: uuid {
    type: number
    value_format_name: id
    sql: ${TABLE}.uuid ;;
  }

  dimension_group: uuid_ts {
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
    sql: ${TABLE}.uuid_ts ;;
  }

  measure: count {
    type: count
    drill_fields: [id, context_library_name]
  }

  measure: count_distinct {
    type: count_distinct
    sql: ${user_id} ;;
    drill_fields: [id, context_library_name]
  }

  measure: avg {
    type: average
    sql:  CAST(${TABLE}.rating AS INT) ;;
    value_format: "#.0"
  }

  measure: promoters {
    type:  count_distinct
    sql: ${user_id} ;;
    filters: {
      field: rating
      value: "10,9"
    }
  }
  measure: passives {
    type:  count_distinct
    sql: ${user_id} ;;
    filters: {
      field: rating
      value: "8,7"
    }
  }

  measure: detractors {
    type:  count_distinct
    sql: ${user_id} ;;
    filters: {
      field: rating
      value: "6,5,4,3,2,1,0"
    }
  }
}
