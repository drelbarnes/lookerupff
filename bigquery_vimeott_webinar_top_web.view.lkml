view: bigquery_vimeott_webinar_top_web {
  sql_table_name: vimeo_ott_webinar.top_web ;;

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.Campaign_Name ;;
  }

  dimension: visit_type {
    type: string
    sql: ${TABLE}.Visit_Type ;;
  }

  dimension: visitor_count {
    type: number
    sql: ${TABLE}.Visitor_count ;;
  }

  measure: count {
    type: count
    drill_fields: [campaign_name]
  }
}
