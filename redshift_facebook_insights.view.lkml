view: redshift_facebook_insights {
  sql_table_name: facebook_ads.insights ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: call_to_action_clicks {
    type: number
    sql: ${TABLE}.call_to_action_clicks ;;
  }

  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }

  measure: clicks_ {
    type: sum
    sql: ${TABLE}.clicks ;;
  }

  dimension: channel {
    type: string
    sql: 'facebook' ;;
  }

  dimension_group: date_start {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.date_start ;;
  }

  dimension_group: date_stop {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.date_stop ;;
  }

  dimension: deeplink_clicks {
    type: number
    sql: ${TABLE}.deeplink_clicks ;;
  }

  dimension: frequency {
    type: number
    sql: ${TABLE}.frequency ;;
  }

  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }

  dimension: inline_post_engagements {
    type: number
    sql: ${TABLE}.inline_post_engagements ;;
  }

  dimension: link_clicks {
    type: number
    sql: ${TABLE}.link_clicks ;;
  }

  measure: deeplink_clicks_ {
    type: sum
    sql: ${TABLE}.deeplink_clicks ;;
  }

  measure: frequency_ {
    type: sum
    sql: ${TABLE}.frequency ;;
  }

  measure: impressions_ {
    type: sum
    sql: ${TABLE}.impressions ;;
  }

  measure: inline_post_engagements_ {
    type: sum
    sql: ${TABLE}.inline_post_engagements ;;
  }

  measure: link_clicks_ {
    type: sum
    sql: ${TABLE}.link_clicks ;;
  }

  dimension_group: loaded {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.loaded_at ;;
  }

  dimension: reach {
    type: number
    sql: ${TABLE}.reach ;;
  }

  measure: reach_ {
    type: sum
    sql: ${TABLE}.reach ;;
  }

  dimension_group: received {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.received_at ;;
  }

  dimension: social_clicks {
    type: number
    sql: ${TABLE}.social_clicks ;;
  }

  dimension: social_impressions {
    type: number
    sql: ${TABLE}.social_impressions ;;
  }

  dimension: social_spend {
    type: number
    sql: ${TABLE}.social_spend ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
  }

  dimension: unique_clicks {
    type: number
    sql: ${TABLE}.unique_clicks ;;
  }

  dimension: unique_impressions {
    type: number
    sql: ${TABLE}.unique_impressions ;;
  }

  dimension: unique_social_clicks {
    type: number
    sql: ${TABLE}.unique_social_clicks ;;
  }

  measure: social_clicks_ {
    type: sum
    sql: ${TABLE}.social_clicks ;;
  }

  measure: social_impressions_ {
    type: sum
    sql: ${TABLE}.social_impressions ;;
  }

  measure: social_spend_ {
    type: sum
    sql: ${TABLE}.social_spend ;;
  }

  measure: spend_ {
    type: sum
    sql: ${TABLE}.spend ;;
  }

  measure: unique_clicks_ {
    type: sum
    sql: ${TABLE}.unique_clicks ;;
  }

  measure: unique_impressions_ {
    type: sum
    sql: ${TABLE}.unique_impressions ;;
  }

  measure: unique_social_clicks_ {
    type: sum
    sql: ${TABLE}.unique_social_clicks ;;
  }

  dimension_group: uuid_ts {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: website_clicks {
    type: number
    sql: ${TABLE}.website_clicks ;;
  }

  measure: website_clicks_ {
    type: sum
    sql: ${TABLE}.website_clicks ;;
  }

  measure: count {
    type: count
    drill_fields: [id]
  }

  measure: count_spend {
    type: sum
    sql: ${spend} ;;
    value_format_name: usd
  }

  measure: count_social_spend {
    type: sum
    sql: ${social_spend} ;;
    value_format_name: usd
  }
}
