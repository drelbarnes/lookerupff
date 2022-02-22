view: bigquery_custom_marketing_spend {
  derived_table: {
    sql: with m as
      (select b.name as ad_name,
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
      INNER JOIN adwords.campaigns  AS d ON c.campaign_id=d.id),

      m1 as
      (select  date(m.timestamp) as timestamp,
             source,
            ad_id,
             campaign_name,
             adset_name,
              ad_name,
             sum(spend) as spend,
             sum(clicks) as clicks,
             sum(impressions) as impressions
      from m
      where (ad_id is not null or ad_id<>'') and ad_name != 'NA'
      group by 1,2,3,4,5,6)

select * from m1
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: adset_name {
    type: string
    sql: ${TABLE}.adset_name ;;
  }

  dimension: ad_name {
    type: string
    sql: ${TABLE}.ad_name ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
  }

  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }

  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }

  set: detail {
    fields: [
      source,
      ad_id,
      campaign_name,
      adset_name,
      ad_name,
      spend,
      clicks,
      impressions
    ]
  }
}
