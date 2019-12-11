view: redshift_marketing_performance_v2 {
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
             source,
             date(m.timestamp) as timestamp,
             sum(spend) as spend,
             sum(clicks) as clicks,
             sum(impressions) as impressions
      from m
      where ad_id is not null or ad_id<>''
      group by 1,2,3,4,5),

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
             timestamp
      from javascript.order_completed
      union all
      select anonymous_id,
            timestamp
      from javascript.conversion),

      a2 as
      (select a.*,
             b.context_page_url as ad_url
      from a0 as a inner join a1 as b on a.anonymous_id=b.anonymous_id and date(a.timestamp)=date(b.timestamp)
      where b.context_page_url<>''),

      w1 as
      (select date(timestamp) as timestamp,
      split_part(split_part(ad_url,'ad_id=',2),'&',1) as ad_id,
             count(distinct anonymous_id) as conversion_count
      from a2
      where split_part(split_part(ad_url,'ad_id=',2),'&',1)<>''
      group by 1,2),

      fb_installs as
            (select anonymous_id,
                   a.timestamp,
                   ad_id,
                   feature as medium,
                   case when platform='ANDROID_APP' then 'android'
                        when platform='IOS_APP' then 'ios' end as platform,
                   'facebook' as channel
            from php.get_app_installs as a),

            google_android_installs as
            (select anonymous_id,
                   a.timestamp,
                   context_aaid as ad_id,
                   context_campaign_medium as medium,
                   'android' as platform,
                   'google' as channel
            from android.branch_install as a),

            google_ios_installs as
            (select anonymous_id,
                   a.timestamp,
                   context_idfa as ad_id,
                   context_campaign_medium as medium,
                   'ios' as platform,
                   'google' as channel
            from ios.branch_install as a),

      i as
      (select * from google_android_installs
      union all
      select * from google_ios_installs
      union all
      select * from fb_installs),

      mi as
      (select anonymous_id,
             order_completed.timestamp,
             'android' as source
      from android.order_completed
      union all
      select anonymous_id,
             conversion.timestamp,
             'android' as source
      from android.conversion
      union all
      select anonymous_id,
             order_completed.timestamp,
             'iOS' as source
      from ios.order_completed
      union all
      select anonymous_id,
             conversion.timestamp,
             'iOS' as source
      from ios.conversion),

      mic as
      (select date(i.timestamp) as timestamp,
             ad_id,
             count(distinct i.anonymous_id) as install_count,
             count(distinct mi.anonymous_id) as mobile_conversions
      from i left join mi on upper(i.anonymous_id)=upper(mi.anonymous_id) and date(i.timestamp)=date(mi.timestamp)
      group by 1,2
      order by 4 desc)

      select m1.*,
             conversion_count as web_conversions,
             install_count as installs,
             mobile_conversions
      from m1 left join w1 on m1.timestamp=w1.timestamp and m1.ad_id=w1.ad_id
              left join mic on m1.timestamp=mic.timestamp and m1.ad_id=mic.ad_id
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

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: timestamp {
    type: date
    sql: ${TABLE}.timestamp ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
    value_format_name: usd
  }

  dimension: clicks {
    type: number
    sql: ${TABLE}.clicks ;;
  }

  dimension: impressions {
    type: number
    sql: ${TABLE}.impressions ;;
  }

  dimension: web_conversions {
    type: number
    sql: ${TABLE}.web_conversions ;;
  }

  dimension: installs {
    type: number
    sql: ${TABLE}.installs ;;
  }

  dimension: mobile_conversions {
    type: number
    sql: ${TABLE}.mobile_conversions ;;
  }

  measure: spend_ {
    type: sum
    sql: ${TABLE}.spend ;;
    value_format_name: usd

  }

  measure: clicks_ {
    type: sum
    sql: ${TABLE}.clicks ;;
  }

  measure: impressions_ {
    type: sum
    sql: ${TABLE}.impressions ;;
  }

  measure: web_conversions_ {
    type: sum
    sql: ${TABLE}.web_conversions ;;
  }

  measure: installs_ {
    type: sum
    sql: ${TABLE}.installs ;;
  }

  measure: mobile_conversions_ {
    type: sum
    sql: ${TABLE}.mobile_conversions ;;
  }

  set: detail {
    fields: [
      ad_id,
      ad_name,
      campaign_name,
      source,
      timestamp,
      spend,
      clicks,
      impressions,
      web_conversions,
      installs,
      mobile_conversions
    ]
  }
}
