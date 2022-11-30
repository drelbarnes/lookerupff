view: hubspot_bogo_testing {
  derived_table: {
    sql: SELECT *
      ,"@bogo.com" as email
      , "web" as platform
      , {% parameter status %} as subscription_status
      , {% parameter event %} as topic
      , {% parameter plan_type %} as frequency
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

  parameter: status {
    type: string
    allowed_value: {
      label: "Free Trial"
      value: "free_trial"
    }
    allowed_value: {
      label: "Subscribed"
      value: "enabled"
    }
  }

  parameter: event {
    type: string
    allowed_value: {
      label: "Free Trial Created"
      value: "customer.product.free_trial_created"
    }
    allowed_value: {
      label: "Free Trial Converted"
      value: "customer.product.free_trial_converted"
    }
    allowed_value: {
      label: "Subscription Created"
      value: "customer.product.created"
    }
    allowed_value: {
      label: "Plan Type Updated"
      value: "customer.product.updated"
    }
    allowed_value: {
      label: "Subscription Renewed"
      value: "customer.product.renewed"
    }
  }

  parameter: plan_type {
    type: string
    allowed_value: {
      label: "Monthly"
      value: "monthly"
    }
    allowed_value: {
      label: "Yearly"
      value: "yearly"
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

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  set: detail {
    fields: [
      email,
      user_id,
      subscription_status,
      topic,
      frequency,
      platform
    ]
  }
}
