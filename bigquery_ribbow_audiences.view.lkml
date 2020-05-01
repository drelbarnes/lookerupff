view: bigquery_ribbow_audiences {
  derived_table: {
    sql: with m
      as
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
      (select ad_id,
             ad_name,
             campaign_name,
             adset_name,
             source,
             date(m.timestamp) as timestamp,
             sum(spend) as spend,
             sum(clicks) as clicks,
             sum(impressions) as impressions
      from m
      where ad_id is not null or ad_id<>''
      group by 1,2,3,4,5,6),

      a as
      (select anonymous_id,
            max(timestamp) as timestamp
      from javascript_upff_home.pages
      group by 1),

      a1 as
      (select a.*, context_page_url
      from a inner join javascript_upff_home.pages as b on a.anonymous_id=b.anonymous_id and a.timestamp=b.timestamp),

      a0 as
      (select anonymous_id,
      user_id,
             timestamp
      from javascript.order_completed
      union all
      select anonymous_id,
      user_id,
            timestamp
      from javascript.conversion),

      a2 as
      (select a.*,
             b.context_page_url as ad_url
      from a0 as a inner join a1 as b on a.anonymous_id=b.anonymous_id and date(a.timestamp)=date(b.timestamp)
      where b.context_page_url<>''),

      w as
      (select date(timestamp) as timestamp,
      anonymous_id,
      user_id,
      split(split(ad_url,'ad_id=')[safe_ordinal(2)],'&')[safe_ordinal(1)] as ad_id
      from a2
      where case when ad_url like '%ad_id=%' then 1 else 0 end =1)

      select distinct w.*,
             ad_name,
             campaign_name,
             adset_name
      from m1 inner join w on m1.timestamp=w.timestamp and m1.ad_id=w.ad_id
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: timestamp {
    type: date
    sql: ${TABLE}.timestamp ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: ad_name {
    type: string
    sql: ${TABLE}.ad_name ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: adset_name {
    type: string
    sql: ${TABLE}.adset_name ;;
  }

  set: detail {
    fields: [
      timestamp,
      anonymous_id,
      user_id,
      ad_id,
      ad_name,
      campaign_name,
      adset_name
    ]
  }
}
