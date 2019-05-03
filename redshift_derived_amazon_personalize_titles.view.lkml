view: redshift_derived_amazon_personalize_titles {
  derived_table: {
    sql: SELECT * FROM `up-faith-and-family-216419.python.users`
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension_group: loaded_at {
    type: time
    sql: ${TABLE}.loaded_at ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: recommended_titles {
    type: string
    hidden: yes
    sql: ${TABLE}.recommended_titles ;;
  }

  dimension_group: uuid_ts {
    type: time
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: recommended_title_five {
    type: string
    hidden: yes
    sql: ${TABLE}.recommended_title_five ;;
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
    sql: ${TABLE}.recommended_title_one ;;
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

  dimension: recommended_title_fi {
    type: string
    label: "recommended_title_five"
    sql: ${TABLE}.recommended_title_fi ;;
  }

  measure: recommendation_one_count {
    type: count
    sql_distinct_key: ${recommended_title_one} ;;
  }

  measure: recommendation_two_count {
    type: count
    sql_distinct_key: ${recommended_title_two} ;;
  }

  measure: recommendation_three_count {
    type: count
    sql_distinct_key: ${recommended_title_three} ;;
  }

  measure: recommendation_four_count {
    type: count
    sql_distinct_key: ${recommended_title_four} ;;
  }

  measure: recommendation_five_count {
    type: count
    sql_distinct_key: ${recommended_title_five} ;;
  }

  set: detail {
    fields: [
      context_library_name,
      context_library_version,
      id,
      loaded_at_time,
      received_at_time,
      recommended_title_five,
      recommended_title_four,
      recommended_title_one,
      recommended_title_three,
      recommended_title_two,
      recommended_title_fi
    ]
  }
}
