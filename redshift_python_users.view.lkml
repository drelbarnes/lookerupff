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


  dimension: recommended_title_six {
    type: string
    sql: ${TABLE}.recommended_title_six ;;
  }


  dimension: recommended_title_seven {
    type: string
    sql: ${TABLE}.recommended_title_seven ;;
  }


  dimension: recommended_title_eight {
    type: string
    sql: ${TABLE}.recommended_title_eight ;;
  }

  dimension: recommended_title_nine {
    type: string
    sql: ${TABLE}.recommended_title_nine ;;
  }


  dimension: recommended_title_ten {
    type: string
    sql: ${TABLE}.recommended_title_ten ;;
  }

  dimension: recommended_title_eleven {
    type: string
    sql: ${TABLE}.recommended_title_eleven ;;
  }


  dimension: recommended_title_five_item_id {
    type: string
    hidden: yes
    sql: ${TABLE}.recommended_title_five_item_id ;;
  }

  dimension: recommended_title_four {
    type: string
    sql: ${TABLE}.recommended_title_four ;;
  }

  dimension: recommended_title_four_item_id {
    type: string
    hidden: yes
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
    hidden: yes
    sql: ${TABLE}.recommended_title_one_item_id ;;
  }

  dimension: recommended_title_three {
    type: string
    sql: ${TABLE}.recommended_title_three ;;
  }

  dimension: recommended_title_three_item_id {
    type: string
    hidden: yes
    sql: ${TABLE}.recommended_title_three_item_id ;;
  }

  dimension: recommended_title_two {
    type: string
    sql: ${TABLE}.recommended_title_two ;;
  }

  dimension: recommended_title_two_item_id {
    type: string
    hidden: yes
    sql: ${TABLE}.recommended_title_two_item_id ;;
  }

  dimension: recommended_titles {
    type: string
    hidden: yes
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

  measure: recommendation_one_count {
    type: count_distinct
    sql: ${id};;
  }

  measure: recommendation_two_count {
    type: count_distinct
    sql: ${id};;
  }

  measure: recommendation_three_count {
    type: count_distinct
    sql: ${id};;
  }

  measure: recommendation_four_count {
    type: count_distinct
    sql: ${id};;
  }

  measure: recommendation_five_count {
    type: count_distinct
    sql: ${id};;
  }
}
