view: redshift_marketing_performance {
  derived_table: {
    sql: select b.name as ad_name,
       c.name as adset_name,
       d.name as campaign_name,
      ad_id,
       date_start as timestamp,
       clicks,
       impressions,
       spend,
       'Facebook' as source
FROM facebook_ads.insights  AS a
INNER JOIN facebook_ads.ads  AS b ON a.ad_id=b.id
INNER JOIN facebook_ads.ad_sets as c ON b.adset_id=c.id
INNER JOIN facebook_ads.campaigns  AS d ON c.campaign_id=d.id

union all

select  'NA' as ad_name,
        c.name as adset_name,
        d.name as campaign_name,
        ad_id,
        date_start as timestamp,
        clicks,
        impressions,
        cost/1000000 as spend,
        'AdWords' as source
FROM adwords.ad_performance_reports  AS a
INNER JOIN adwords.ads  AS b ON a.ad_id=b.id
INNER JOIN adwords.ad_groups  AS c ON c.id=b.ad_group_id
INNER JOIN adwords.campaigns  AS d ON c.campaign_id=d.id
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: ad_name {
    type: string
    sql: ${TABLE}.ad_name ;;
  }

  dimension: adset_name {
    type: string
    sql: ${TABLE}.adset_name ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }

  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }

  measure: clicks_ {
    type: sum
    sql: ${TABLE}.clicks ;;
  }

  measure: impressions_ {
    type: sum
    sql: ${TABLE}.impressions ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
    value_format_name: usd
  }

  measure: spend_ {
    type: sum
    sql: ${TABLE}.spend ;;
    value_format_name: usd
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  set: detail {
    fields: [
      ad_name,
      adset_name,
      campaign_name,
      timestamp_time,
      clicks,
      impressions,
      spend,
      source
    ]
  }
}
