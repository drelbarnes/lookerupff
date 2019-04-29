view: redshift_python_users {
  sql_table_name: python.users ;;

  dimension: id {
    primary_key: yes
    tags: ["user_id"]
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

  dimension: recommended_title_five {
    type: string
    sql: ${TABLE}.recommended_title_five ;;
  }

  dimension: recommended_title_five_item_id {
    type: string
    sql: ${TABLE}.recommended_title_five_item_id ;;
  }

  dimension: recommended_title_four {
    type: string
    sql: ${TABLE}.recommended_title_four ;;
  }

  dimension: recommended_title_four_item_id {
    type: string
    sql: ${TABLE}.recommended_title_four_item_id ;;
  }

  dimension: recommended_title_one {
    type: string
    sql: CAST(${TABLE}.recommended_title_one as INT) ;;
  }

  dimension: titleOne {
    type: number
    value_format: "0"
    sql: ${recommended_title_one} ;;
  }

  dimension: recommended_title_one_item_id {
    type: string
    sql: ${TABLE}.recommended_title_one_item_id ;;
  }

  dimension: recommended_title_three {
    type: string
    sql: ${TABLE}.recommended_title_three ;;
  }

  dimension: recommended_title_three_item_id {
    type: string
    sql: ${TABLE}.recommended_title_three_item_id ;;
  }

  dimension: recommended_title_two {
    type: string
    sql: ${TABLE}.recommended_title_two ;;
  }

  dimension: recommended_title_two_item_id {
    type: string
    sql: ${TABLE}.recommended_title_two_item_id ;;
  }

  dimension: recommended_titles {
    type: string
    sql: ${TABLE}.recommended_titles ;;
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
