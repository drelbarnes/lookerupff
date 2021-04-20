view: bigquery_email_sends {
  derived_table: {
    sql: SELECT distinct e.email, e.campaign_id, c.campaign_send_time FROM `up-faith-and-family-216419.customers.email_sends` AS e LEFT JOIN php.get_email_campaigns AS c ON e.campaign_id = c.campaign_id
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension_group: campaign_send_time {
    type: time
    sql: ${TABLE}.campaign_send_time ;;
  }

  set: detail {
    fields: [email, campaign_id, campaign_send_time_time]
  }
}
