view: bigquery_app_installs_by_platform {
  derived_table: {
    sql: with android_fb as
      (select distinct a.id,
                      advertising_partner_name,
                      b.timestamp,
                      "Android" as source
      from android.users as a inner join php.get_app_installs as b on context_traits_anonymous_id=b.anonymous_id or aaid= context_device_advertising_id or context_ip= ip
      where date(b.timestamp)>='2019-02-01' ),

      ios_fb as
      (select distinct a.id,
                      advertising_partner_name,
                      b.timestamp,
                      "iOS" as source
      from ios.users as a inner join php.get_app_installs as b on idfa= context_device_advertising_id or context_ip= ip
      where date(b.timestamp)>='2019-02-01'),

      android_google as
      (select distinct a.id,
                       advertising_partner_name,
                       b.timestamp,
                       "Android" as source
      from android.users as a inner join android.branch_install as b on context_traits_anonymous_id=anonymous_id or context_aaid=context_device_advertising_id or a.context_ip=b.context_ip),

      ios_google as
      (select distinct a.id,
                       " " as advertising_partner_name,
                       b.timestamp,
                      "iOS" as source
      from ios.users as a inner join ios.branch_install as b on context_device_advertising_id=context_idfa or a.context_ip=b.context_ip)

      select *
      from android_fb
      union all
      select *
      from android_google
      union all
      select *
      from ios_fb
      union all
      select *
      from ios_google
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
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
