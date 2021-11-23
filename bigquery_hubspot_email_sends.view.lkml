view: bigquery_hubspot_email_sends {
  derived_table: {
    sql:
    SELECT distinct events.recipient, events.email_campaign_id, campaigns.last_processing_finished_at
    FROM hubspot.email_events as events
    LEFT JOIN hubspot.email_campaigns as campaigns ON CAST(events.email_campaign_id AS string)=CAST(campaigns.id AS string) ;;
  }

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    sql: ${TABLE}.recipient ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.email_campaign_id ;;
  }

  dimension_group: campaign_send_time {
    type: time
    sql: ${TABLE}.last_processing_finished_at ;;
  }

  measure: distinct_count {
    type: count_distinct
    sql:  ${email};;
  }

  set: detail {
    fields: [email, campaign_id, campaign_send_time_time]
  }
}
