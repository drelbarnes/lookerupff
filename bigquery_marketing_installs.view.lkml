view: bigquery_marketing_installs {
  derived_table: {
    sql: with fb_installs as
      (select anonymous_id,
             timestamp,
             case when aaid is null and idfa is null then null
                  when aaid is null and idfa is not null then idfa
                  when aaid is not null and idfa is null then aaid end as ad_id,
             campaign,
             feature as medium,
             case when platform='ANDROID_APP' then 'android'
                  when platform='IOS_APP' then 'ios' end as platform,
             'facebook' as channel
      from php.get_app_installs),

      google_android_installs as
      (select anonymous_id,
             timestamp,
             context_aaid as ad_id,
             context_campaign_name as campaign,
             context_campaign_medium as medium,
             'android' as platform,
             'google' as channel
      from android.branch_install),

      google_ios_installs as
      (select anonymous_id,
             timestamp,
             context_idfa as ad_id,
             context_campaign_name as campaign,
             context_campaign_medium as medium,
             'ios' as platform,
             'google' as channel
      from ios.branch_install),

a as
      (select * from google_android_installs
      union all
      select * from google_ios_installs
      union all
      select * from fb_installs)

(select a.*,
       case when name is null then 'organic' else 'paid' end as type
from a left join facebook_ads.campaigns as b on a.campaign=name)
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
    }

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: medium {
    type: string
    sql: ${TABLE}.medium ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  measure: installation_count {
    type: count_distinct
    sql: ${anonymous_id} ;;
  }

  set: detail {
    fields: [
      anonymous_id,
      timestamp_time,
      ad_id,
      medium,
      platform,
      channel
    ]
  }
}
