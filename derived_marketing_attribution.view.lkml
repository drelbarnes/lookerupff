view: derived_marketing_attribution {
  derived_table: {
    sql: with

                  android as
                  (select a.timestamp as visitingtimestamp,
                          os as platform,
                        -- advertising_partner_name as trafficchannel,
                        context_campaign_source as trafficchanneltype,
                        context_campaign_name,
                        context_Campaign_medium,
                        context_campaign_source,
                        b.id
                  from android.branch_install as a inner join android.users as b on
                  ip = context_ip)
                  ,

                  android_ as
                  (select a.timestamp as visitingtimestamp,
                          os as platform,
                        -- advertising_partner_name as trafficchannel,
                        context_campaign_source as trafficchanneltype,
                        context_campaign_name,
                        context_Campaign_medium,
                        context_campaign_source,
                        b.id
                  from android.branch_reinstall as a inner join android.users as b on
                  ip = context_ip),

                  ios as
                  (select a.timestamp as visitingtimestamp,
                        os as platform,
                        -- advertising_partner_name as trafficchannel,
                        context_campaign_source as trafficchanneltype,
                        context_campaign_name,
                        context_Campaign_medium,
                        context_campaign_source,
                        b.id
                  from ios.branch_install as a inner join ios.users as b on
                  ip = context_ip),

                  ios_ as
                  (select a.timestamp as visitingtimestamp,
                        os as platform,
                        -- advertising_partner_name as trafficchannel,
                        context_campaign_source as trafficchanneltype,
                        context_campaign_name,
                        context_Campaign_medium,
                        context_campaign_source,
                        b.id
                  from ios.branch_reinstall as a inner join ios.users as b on
                  ip = context_ip),

                  web as
                  (select a.timestamp as visitingtimestamp,
                        'web' as platform,
                        context_campaign_source as trafficchanneltype,
                        context_campaign_name,
                        context_Campaign_medium,
                        context_campaign_source,
                        user_id as id
                  from javascript.subscribed as a)

                  (select * from android
                  union all
                  select * from android_
                  union all
                  select * from ios
                  union all
                  select * from ios_
                  union all
                  select * from web)


                  -- select distinct context_campaign_name, context_campaign_source, context_campaign_content, context_Campaign_medium from javascript.start_checkout
                  -- union all
                  -- select distinct context_campaign_name, context_campaign_source, context_campaign_content, context_Campaign_medium  from android.branch_install
                  -- union all
                  -- select distinct context_campaign_name, context_campaign_source, context_campaign_content, context_Campaign_medium  from ios.branch_install
                   ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: visitingtimestamp {
    type: time
    sql: ${TABLE}.visitingtimestamp ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: trafficchanneltype {
    type: string
    sql: ${TABLE}.trafficchanneltype ;;
  }

  dimension: context_campaign_name {
    type: string
    sql: ${TABLE}.context_campaign_name ;;
  }

  dimension: context_campaign_medium {
    type: string
    sql: ${TABLE}.context_campaign_medium ;;
  }

  dimension: context_campaign_source {
    type: string
    sql: ${TABLE}.context_campaign_source ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  set: detail {
    fields: [
      visitingtimestamp_time,
      platform,
      trafficchanneltype,
      context_campaign_name,
      context_campaign_medium,
      context_campaign_source,
      id
    ]
  }
}
