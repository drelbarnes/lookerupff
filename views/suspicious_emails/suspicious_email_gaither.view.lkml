view: suspicious_email_gaither {derived_table:{
    sql:
      select
          content_subscription_billing_period_unit
          ,content_customer_billing_address_zip
          ,content_card_first_name
          ,content_card_last_name
          ,content_customer_email
          ,content_subscription_id
          ,content_customer_created_from_ip
          ,timestamp
          from chargebee_webhook_events.subscription_created
          WHERE content_customer_email LIKE (lower(content_customer_first_name)||'.'||lower(content_customer_last_name) ||'________________'||'%@%');;


  }
  dimension: date {
    type: date
    sql:  ${TABLE}.timestamp ;;
  }

  dimension: zipcode {
    type: number
    sql:  ${TABLE}.content_customer_billing_address_zip ;;
  }

  dimension: first_name {
    type: string
    sql:  ${TABLE}.content_card_first_name ;;
  }
  dimension: last_name {
    type: string
    sql:  ${TABLE}.content_card_last_name ;;
  }
  dimension: email {
    type: string
    sql:  ${TABLE}.content_customer_email ;;
  }
  dimension: subscription_id {
    type: string
    sql:  ${TABLE}.content_subscription_id;;
  }

  dimension: ip_address {
    type: string
    sql:  ${TABLE}.content_customer_created_from_ip;;
  }

  dimension: subscription_plan {
    type: string
    sql:  ${TABLE}.content_subscription_billing_period_unit;;
  }
  }
