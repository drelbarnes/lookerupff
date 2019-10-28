view: bigquery_vimeott_webinar_ads {
  sql_table_name: vimeo_ott_webinar.ads_v2 ;;

  dimension: ad_name {
    type: string
    sql: ${TABLE}.Ad_Name ;;
  }

  dimension: ad_set_name {
    type: string
    sql: ${TABLE}.Ad_Set_Name ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.Campaign_Name ;;
  }

  dimension: clicks {
    type: number
    sql: ${TABLE}.Clicks ;;
  }

  dimension: cost_per_clicks {
    type: number
    sql: ${TABLE}.Cost_Per_Clicks ;;
  }

  dimension: impressions {
    type: number
    sql: ${TABLE}.Impressions ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.Source ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.Spend ;;
  }

  measure: count {
    type: count
    drill_fields: [campaign_name, ad_set_name, ad_name]
  }
}
