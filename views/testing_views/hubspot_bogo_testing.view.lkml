view: hubspot_bogo_testing {
  derived_table: {
    sql: SELECT *
      ,"@bogo.com" as email
      FROM UNNEST(GENERATE_ARRAY(1, 100)) as user_id
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: user_id {
    type: string
    tags: ["user_id"]
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  set: detail {
    fields: [
      email,
      user_id
    ]
  }
}
