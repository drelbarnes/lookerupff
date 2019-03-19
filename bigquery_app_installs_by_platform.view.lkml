view: bigquery_app_installs_by_platform {
  derived_table: {
    sql:  with android_fb1 as
      (select distinct a.anonymous_id,
                      "Install" as event,
                      advertising_partner_name,
                      b.timestamp,
                      "Android" as source
      from android.application_installed as a inner join php.get_app_installs as b on context_traits_anonymous_id=b.anonymous_id or aaid= context_device_advertising_id or context_ip= ip
      where date(b.timestamp)>='2019-02-01' ),

      android_fb2 as
      (select distinct a.anonymous_id,
                      "Reinstall" as event,
                      advertising_partner_name,
                      b.timestamp,
                      "Android" as source
      from android.application_installed as a inner join php.get_app_reinstalls as b on context_traits_anonymous_id=b.anonymous_id or aaid= context_device_advertising_id or context_ip= ip
      where date(b.timestamp)>='2019-02-01' ),

      ios_fb1 as
      (select distinct a.anonymous_id,
                      "Install" as event,
                      advertising_partner_name,
                      b.timestamp,
                      "iOS" as source
      from ios.application_installed as a inner join php.get_app_installs as b on a.anonymous_id=b.anonymous_id or idfa= context_device_advertising_id or context_ip= ip
      where date(b.timestamp)>='2019-02-01'),

      ios_fb2 as
      (select distinct a.anonymous_id,
                      "Reinstall" as event,
                      advertising_partner_name,
                      b.timestamp,
                      "iOS" as source
      from ios.application_installed as a inner join php.get_app_reinstalls as b on a.anonymous_id=b.anonymous_id or idfa= context_device_advertising_id or context_ip= ip
      where date(b.timestamp)>='2019-02-01'),

      android_google1 as
      (select distinct a.anonymous_id,
                       "Install" as event,
                       advertising_partner_name,
                       b.timestamp,
                       "Android" as source
      from android.application_installed as a inner join android.branch_install as b on a.anonymous_id=b.anonymous_id or context_aaid=context_device_advertising_id or a.context_ip=b.context_ip
      where date(b.timestamp)>='2019-02-01'),

      android_google2 as
      (select distinct a.anonymous_id,
                       "Reinstall" as event,
                       advertising_partner_name,
                       b.timestamp,
                       "Android" as source
      from android.application_installed as a inner join android.branch_reinstall as b on a.anonymous_id=b.anonymous_id or context_aaid=context_device_advertising_id or a.context_ip=b.context_ip
      where date(b.timestamp)>='2019-02-01'),

      ios_google1 as
      (select distinct a.anonymous_id,
                      "Install" as event,
                      advertising_partner_name,
                       b.timestamp,
                      "iOS" as source
      from ios.application_installed as a inner join ios.branch_open as b on context_device_advertising_id=context_idfa or a.context_ip=b.context_ip or a.anonymous_id=b.anonymous_id inner join ios.branch_install as c on context_device_advertising_id=c.context_idfa or a.context_ip=b.context_ip or a.anonymous_id=c.anonymous_id
      where date(b.timestamp)>='2019-02-01' ),

      ios_google2 as
      (select distinct a.anonymous_id,
                       "Reinstall" as event,
                       advertising_partner_name,
                       b.timestamp,
                      "iOS" as source
      from ios.application_installed as a inner join ios.branch_open as b on context_device_advertising_id=context_idfa or a.context_ip=b.context_ip or a.anonymous_id=b.anonymous_id inner join ios.branch_reinstall as c on context_device_advertising_id=c.context_idfa or a.context_ip=b.context_ip
      where date(b.timestamp)>='2019-02-01' )

      select *
      from android_fb1
      union all
      select *
      from android_google1
      union all
      select *
      from ios_fb1
      union all
      select *
      from ios_google1
      union all
      select *
      from android_fb2
      union all
      select *
      from android_google2
      union all
      select *
      from ios_fb2
      union all
      select *
      from ios_google2

 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: event{
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: advertising_partner_name {
    type: string
    sql: ${TABLE}.advertising_partner_name ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  measure: audience_size {
    type: count_distinct
    sql:  ${id};;
  }

  set: detail {
    fields: [id, advertising_partner_name, timestamp_time, source]
  }
}
