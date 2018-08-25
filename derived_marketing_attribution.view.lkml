view: derived_marketing_attribution {
  derived_table: {
    sql:  with

                        android as
                        (select a.timestamp as visitingtimestamp,
                                os as platform,
                              -- advertising_partner_name as trafficchannel,
                              context_campaign_source as trafficchanneltype,
                              context_campaign_name,
                              context_Campaign_medium,
                              c.id,
                              anonymous_id,
                              session_id,
                              context_timezone
                        from android.view as a left join android.branch_install as b on a.context_ip = b.ip  inner join android.users as c
                        on b.ip = c.context_ip)
                        ,

                        android_ as
                        (select a.timestamp as visitingtimestamp,
                                os as platform,
                              -- advertising_partner_name as trafficchannel,
                              context_campaign_source as trafficchanneltype,
                              context_campaign_name,
                              context_Campaign_medium,
                              c.id,
                              anonymous_id,
                              session_id,
                              context_timezone
                        from android.view as a left join android.branch_install as b on a.context_ip = b.ip  inner join android.users as c
                        on b.ip = c.context_ip)
                        ,

                        ios as
                        (select a.timestamp as visitingtimestamp,
                              os as platform,
                              -- advertising_partner_name as trafficchannel,
                              context_campaign_source as trafficchanneltype,
                              context_campaign_name,
                              context_Campaign_medium,
                              c.id,
                              anonymous_id,
                              session_id,
                              context_timezone
                        from ios.view as a left join ios.branch_install as b on a.context_ip = b.ip  inner join ios.users as c
                        on b.ip = c.context_ip)
                        ,

                        ios_ as
                        (select a.timestamp as visitingtimestamp,
                              os as platform,
                              -- advertising_partner_name as trafficchannel,
                              context_campaign_source as trafficchanneltype,
                              context_campaign_name,
                              context_Campaign_medium,
                              c.id,
                              anonymous_id,
                              session_id,
                              context_timezone
                        from ios.view as a left join ios.branch_install as b on a.context_ip = b.ip  inner join ios.users as c
                        on b.ip = c.context_ip)
                        ,

                        web as
                        (select a.timestamp as visitingtimestamp,
                              'web' as platform,
                              a.context_campaign_source as trafficchanneltype,
                              a.context_campaign_name,
                              a.context_Campaign_medium,
                              b.user_id as id,
                              anonymous_id,
                              context_timezone
                        from javascript.pages as a left join javascript.subscribed as b on a.anonymous_id = b.anonymous_id)

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

dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: context_timezone {
    type: string
    sql: ${TABLE}.context_timezone ;;
  }

  set: detail {
    fields: [
      visitingtimestamp_time,
      platform,
      trafficchanneltype,
      context_campaign_name,
      context_campaign_medium,
      id,
      anonymous_id,
      session_id,
      context_timezone
    ]
  }
}
