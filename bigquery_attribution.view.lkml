view: bigquery_attribution {
  derived_table: {
    sql: with branch_ios as
      (select a.anonymous_id as anonymous_id_a,
            a.context_idfa as idfa_a,
            a.context_ip as ip_a,
            advertising_partner_name,
            b.timestamp
      from ios.branch_open as a inner join ios.branch_install as b on a.context_idfa=b.context_idfa or a.anonymous_id=b.anonymous_id or a.context_ip=b.context_ip
      where date(b.timestamp)>=date_sub(current_date(),interval 30 day) and (a.anonymous_id<>'anonymous')),

      branch_ios2 as
      (select a.*,
             b.id as user_id,
             'iOS' as platform
      from branch_ios as a inner join ios.users as b on idfa_a=context_device_advertising_id),

      branch_android as
      (select anonymous_id,
            context_aaid,
            a.context_ip,
            advertising_partner_name,
            a.timestamp,
            b.id as user_id,
            'Android' as platform
      from  android.branch_install as a inner join android.users as b on context_aaid=context_device_advertising_id or a.context_ip=b.context_ip or a.anonymous_id=b.context_traits_anonymous_id
      where date(timestamp)>=date_sub(current_date(),interval 30 day) and (a.context_aaid is not null or context_device_advertising_id is not null or a.anonymous_id=b.context_traits_anonymous_id or a.context_ip=b.context_ip)),

      php_android as
      (select anonymous_id,
             aaid,
             ip,
             advertising_partner_name,
             a.timestamp,
             b.id as user_id,
             'Android' as platform
      from php.get_app_installs as a inner join android.users as b on aaid=context_device_advertising_id or context_ip=ip or a.anonymous_id=b.context_traits_anonymous_id
      where date(timestamp)>=date_sub(current_date(),interval 30 day) and (a.aaid is not null or context_device_advertising_id is not null or a.anonymous_id=b.context_traits_anonymous_id or a.ip=b.context_ip)),

      php_ios as
      (select anonymous_id,
             idfa,
             ip,
             advertising_partner_name,
             a.timestamp,
             b.id as user_id,
             'iOS' as platform
      from php.get_app_installs as a inner join ios.users as b on aaid=context_device_advertising_id or context_ip=ip
      where date(timestamp)>=date_sub(current_date(),interval 30 day) and (a.aaid is not null or context_device_advertising_id is not null or a.ip=b.context_ip))

      select * from php_android
      union all
      select * from branch_android
      union all
      select * from php_ios
      union all
      select * from branch_ios2
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: distinct_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: aaid {
    type: string
    sql: ${TABLE}.aaid ;;
  }

  dimension: ip {
    type: string
    sql: ${TABLE}.ip ;;
  }

  dimension: advertising_partner_name {
    type: string
    sql: ${TABLE}.advertising_partner_name ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  set: detail {
    fields: [
      anonymous_id,
      aaid,
      ip,
      advertising_partner_name,
      timestamp_time,
      user_id,
      platform
    ]
  }
}
