view: bigquery_free_to_paid {
  derived_table: {
    sql: select
      c.user_id, p.email, p.topic, p.context_campaign_name, p.anonymous_id,
             pe.received_at,
             "Web" as os
      from biqquery_javascript_pages AS p, bigquery_javascript_conversion AS c, bigquery_http_api_purchase_event AS pe WHERE p.anonymous_id = c.anonymous_id AND c.email = pe.email;;
  }


  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic;;
  }

  dimension: context_campaign_name {
    type: string
    sql: ${TABLE}.context_campaign_name;;
  }

  measure: count {
    type: count_distinct
    sql: ${anonymous_id} ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: os {
    type: string
    sql: ${TABLE}.os ;;
  }

  set: detail {
    fields: [anonymous_id, received_at_time, os]
  }
}
