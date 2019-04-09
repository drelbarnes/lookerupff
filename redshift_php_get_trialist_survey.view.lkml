view: redshift_php_get_trialist_survey {
  sql_table_name: php.get_trialist_survey ;;

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
    type:string
    sql: ${TABLE}.rating ;;
  }

  dimension: total_rating {
    type: number
    label: "Rating Number"
    value_format: "0"
    sql:  CAST(${TABLE}.rating AS INT) ;;
  }

  dimension: total_nps {
    type: number
    sql:  SUM(CAST(${TABLE}.rating AS INT)) ;;
  }

  dimension: reason {
    type: string
    sql: ${TABLE}.reason ;;
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

#Get Promoters by case
  dimension: nps_cat {
    case: {
      when: {
        sql: CAST(${rating} AS INT) = 9 OR
          CAST(${rating} AS INT) = 10 ;;
        label: "Promoters"
      }
      when: {
        sql: CAST(${rating} AS INT)  = 7 OR
          CAST(${rating} AS INT)  = 8 ;;
        label: "Passives"
      }
      when: {
        sql: CAST(${rating} AS INT) <= 6 ;;
        label: "Detractors"
      }
      else: "Nevers"
    }
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


  measure: count {
    type: count
    drill_fields: [id, context_library_name]
  }

  measure: nps_total {
    type:  sum
    sql:  CAST(${rating} AS INT) ;;
  }


  measure: count_distinct {
    type: count_distinct
    sql: ${user_id} ;;
    drill_fields: [id, context_library_name]
  }

  measure: avg {
    type: number
    sql: (${promoters} - ${detractors}) / ${count_distinct} * 100 ;;
  }
}
