view: derived_marketing_attribution {
  derived_table: {
    sql:  with

                                 android as
                                    (select  b.timestamp as visitingtimestamp,
                                            user_data_os as platform,
                                            a.name AS event,
                                          -- advertising_partner_name as trafficchannel,
                                          last_attributed_touch_data_tilde_advertising_partner_name AS context_campaign_source,
                                          last_attributed_touch_data_tilde_campaign AS context_campaign_name,
                                          last_attributed_touch_data_tilde_channel AS context_Campaign_medium,
                                          last_attributed_touch_data_tilde_ad_set_name AS ad_set_name,
                                          last_attributed_touch_data_tilde_ad_set_id AS ad_set_id,
                                          last_attributed_touch_data_tilde_ad_name AS ad_name,
                                          last_attributed_touch_data_tilde_ad_id AS ad_id,
                                          c.id
                                    from customers.social_ads as a inner join android.signupstarted as b
                                    on a.user_data_aaid = b.context_device_advertising_id inner join android.users as c on b.context_device_advertising_id = c.context_device_advertising_id WHERE context_Campaign_medium = 'Facebook')
                                    ,
                                    android_install as
                                    (select  b.timestamp as visitingtimestamp,
                                            os as platform,
                                            'INSTALL' AS event,
                                          -- advertising_partner_name as trafficchannel,
                                          context_campaign_source as trafficchanneltype,
                                          context_campaign_name,
                                          context_Campaign_medium,
                                          '' AS ad_set_name,
                                          '' AS ad_set_id,
                                          '' AS ad_name,
                                          '' AS ad_id,
                                         c.id
                                    from android.branch_install as a inner join android.signupstarted as b
                                    on a.context_ip = b.context_ip inner join android.users as c on b.context_ip = c.context_ip)
                                    ,
                                    android_reinstall as
                                    (select  b.timestamp as visitingtimestamp,
                                            os as platform,
                                            'REINSTALL' AS event,
                                          -- advertising_partner_name as trafficchannel,
                                          context_campaign_source as trafficchanneltype,
                                          context_campaign_name,
                                          context_Campaign_medium,
                                          '' AS ad_set_name,
                                          '' AS ad_set_id,
                                          '' AS ad_name,
                                          '' AS ad_id,
                                          c.id
                                    from android.branch_reinstall as a inner join android.signupstarted as b
                                    on a.context_ip = b.context_ip inner join android.users as c on b.context_ip = c.context_ip)
                                    ,
                                    ios as
                                    (select  b.timestamp as visitingtimestamp,
                                            user_data_os as platform,
                                            a.name AS event,
                                          -- advertising_partner_name as trafficchannel,
                                          last_attributed_touch_data_tilde_advertising_partner_name AS context_campaign_source,
                                          last_attributed_touch_data_tilde_campaign AS context_campaign_name,
                                          last_attributed_touch_data_tilde_channel AS context_Campaign_medium,
                                          last_attributed_touch_data_tilde_ad_set_name AS ad_set_name,
                                          last_attributed_touch_data_tilde_ad_set_id AS ad_set_id,
                                          last_attributed_touch_data_tilde_ad_name AS ad_name,
                                          last_attributed_touch_data_tilde_ad_id AS ad_id,
                                          c.id
                                    from customers.social_ads as a inner join ios.signupstarted as b
                                    on a.user_data_idfa = b.context_device_advertising_id inner join ios.users as c on b.context_device_advertising_id = c.context_device_advertising_id WHERE context_Campaign_medium = 'Facebook')
                                    ,
                                    web as
                                    (select  a.original_timestamp as visitingtimestamp,
                                          'web' as platform,
                                          '' AS event,
                                          a.context_campaign_source,
                                          a.context_campaign_name,
                                          a.context_Campaign_medium,
                                          '' AS ad_set_name,
                                          '' AS ad_set_id,
                                          '' AS ad_name,
                                          '' AS ad_id,
                                          b.id
                                    from javascript.subscribed as a inner join javascript.users as b on a.user_id = b.id)

                                    (select * from android
                                    union all
                                    select * from android_install
                                    union all
                                    select * from android_reinstall
                                    union all
                                    select * from ios
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

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: ad_set_name {
    type: string
    sql: ${TABLE}.ad_set_name ;;
  }

  dimension: ad_set_id {
    type: number
    sql: ${TABLE}.ad_set_id ;;
  }

  dimension: ad_name {
    type: string
    sql: ${TABLE}.ad_name ;;
  }

  dimension: ad_id {
    type: number
    sql: ${TABLE}.ad_id ;;
  }

  dimension: context_campaign_source {
    type: string
    sql: ${TABLE}.context_campaign_source ;;
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
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  set: detail {
    fields: [
      visitingtimestamp_time,
      platform,
      context_campaign_source,
      context_campaign_name,
      context_campaign_medium,
      id
    ]
  }
}
