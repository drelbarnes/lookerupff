view: redshift_python_users {
  sql_table_name: python.users ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
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

  dimension: recommended_titles {
    type: string
    sql: ${TABLE}.recommended_titles ;;
  }

  dimension: recommended_content {
    type: string
    sql: SPLIT_PART(${recommended_titles}, ',', 10);;
  }

  dimension: recommended_title {
    type: string
    sql: replace(${recommended_content}, ']', '');;
  }

  dimension: titles_ids {
    type: number
    value_format: "0"
    sql: CAST(replace(${recommended_title}, '"', '') as INT);;
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
}
