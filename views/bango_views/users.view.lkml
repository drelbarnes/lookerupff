# The name of this view in Looker is "Users"
view: users {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: admin_bang_prod.users ;;
  drill_fields: [id]
  # This primary key is the unique key for this table in the underlying database.
  # You need to define a primary key in a view in order to join to other views.

  # derived_table: {
  #   sql:
  #     select
  #     bango_user_Id
  #     , customer_id
  #     , date_activated as date_activated_test
  #     , str_to_date(date_activated, '%Y-%m-%dT%H:%i:%s') as date_activated
  #     , str_to_date(date_created, '%Y-%m-%dT%H:%i:%s') as date_created
  #     , str_to_date(date_suspended, '%Y-%m-%dT%H:%i:%s') as date_suspended
  #     , str_to_date(date_resumed, '%Y-%m-%dT%H:%i:%s') as date_resumed
  #     , str_to_date(date_ended, '%Y-%m-%dT%H:%i:%s') as date_ended
  #     , email_hashed
  #     , entitlement_id
  #     , offer_key
  #     , partner_id
  #     , product_key
  #     , reseller_key
  #     , timestamp
  #     , id
  #     from admin_bang_prod.users
  #     ;;
  # }

  parameter: free_trial_length {
    label: "Free Trial Length"
    type: number
    default_value: "14"
    allowed_value: {
      label: "No Free Trial"
      value: "0"
    }
    allowed_value: {
      label: "7 days"
      value: "7"
    }
    allowed_value: {
      label: "14 days"
      value: "14"
    }
  }

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: bango_user_id {
    type: number
    sql: ${TABLE}.bango_user_Id ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: ${TABLE}.customer_id ;;
  }

  dimension_group: date_activated {
    type: time
    sql: ${TABLE}.date_activated ;;
  }

  dimension_group: date_created {
    type: time
    sql: ${TABLE}.date_created ;;
  }

  dimension_group: date_ended {
    type: time
    sql: ${TABLE}.date_ended ;;
  }

  dimension_group: date_resumed {
    type: time
    sql: ${TABLE}.date_resumed ;;
  }

  dimension_group: date_suspended {
    type: time
    sql: ${TABLE}.date_suspended ;;
  }

  dimension: email_hashed {
    type: string
    sql: ${TABLE}.email_hashed ;;
  }

  dimension: entitlement_id {
    type: string
    sql: ${TABLE}.entitlement_id ;;
  }

  dimension: offer_key {
    type: string
    sql: ${TABLE}.offer_key ;;
  }

  dimension: partner_id {
    type: number
    # hidden: yes
    sql: ${TABLE}.partner_id ;;
  }

  dimension: product_key {
    type: string
    sql: ${TABLE}.product_key ;;
  }

  dimension: reseller_key {
    type: string
    sql: ${TABLE}.reseller_key ;;
  }

  dimension: churn_date_difference {
    type: number
    hidden: yes
    sql: DATEDIFF(${date_ended_time},${date_activated_time}) ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: churn_type {
    type: yesno
  }

  measure: count {
    type: count
    drill_fields: [id, partners.id]
  }

  measure: free_trials {
    type: count_distinct
    sql: ${entitlement_id} ;;
    filters: [date_activated_date: "last 14 days"]
  }

  measure: free_trial_expired {
    type: count_distinct
    sql: ${entitlement_id} ;;
    filters: [churn_date_difference: "<=15"]
    # date_ended_date - date_activated_date < 15 days
  }

  measure: subscribers {
    type: count_distinct
    sql: ${entitlement_id} ;;
    hidden: yes
    filters: [date_activated_date: "before 13 days ago"]
  }

  measure: paying_churn {
    type: count_distinct
    sql: ${entitlement_id} ;;
    filters: [churn_date_difference: ">15"]
    # date_ended_date - date_activated_date > 15 days
  }

  measure: paying_subscribers {
    type: number
    sql: ${subscribers}-${paying_churn} ;;
  }

  measure: total_subscribers {
    type: number
    sql: ${paying_subscribers} + ${free_trials} ;;
  }

  dimension: today {
    type: date
    sql: CURRENT_DATE ;;
  }

  dimension: ref_date {
    type: date
    sql: ${timestamp_date} ;;
  }

  dimension: date_diff {
    type: number
    sql: DATEDIFF(${today},${ref_date}) ;;
  }

      measure: daily_count {
      type: number
      sql: ${paying_subscribers} + ${free_trials} ;;
    }
  }
