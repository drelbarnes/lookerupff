view: hubspot_bogo_testing {
  derived_table: {
    sql: SELECT *
      ,"@bogo.com" as email
      FROM UNNEST(GENERATE_ARRAY(1, {% parameter number_of_results %})) as user_id
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }
  parameter: number_of_results {
    type: unquoted
    default_value: "100"
    allowed_value: {
      label: "10"
      value: "10"
    }
    allowed_value: {
      label: "50"
      value: "50"
    }
    allowed_value: {
      label: "100"
      value: "100"
    }
    allowed_value: {
      label: "250"
      value: "250"
    }
    allowed_value: {
      label: "500"
      value: "500"
    }
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
