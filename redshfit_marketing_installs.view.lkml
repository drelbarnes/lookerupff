view: redshfit_marketing_installs {
  derived_table: {
    sql: with fb_installs as
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
             creative_id as ad_id,
             context_campaign_medium as medium,
             'android' as platform,
             'google' as channel
      from android.branch_install as a),

      google_ios_installs as
      (select anonymous_id,
             a.timestamp,
             creative_id as ad_id,
             context_campaign_medium as medium,
             'ios' as platform,
             'google' as channel
      from ios.branch_install as a)

      select * from google_android_installs
      union all
      select * from google_ios_installs
      union all
      select * from fb_installs
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

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
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
