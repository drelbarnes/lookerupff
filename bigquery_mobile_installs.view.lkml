view: bigquery_mobile_installs {
  derived_table: {
    sql: with a1 as
      (select anonymous_id,
             ip,
             aaid,
             case when advertising_partner_name is null then 'organic' else advertising_partner_name end as advertising_partner_name,
             campaign,
             timestamp
      from php.get_app_installs
      union all
      select anonymous_id,
             ip,
             aaid,
             case when advertising_partner_name is null then 'organic' else advertising_partner_name end as advertising_partner_name,
             campaign,
             timestamp
      from php.get_app_reinstalls)

      select * from a1
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

  dimension: ip {
    type: string
    sql: ${TABLE}.ip ;;
  }

  dimension: aaid {
    type: string
    sql: ${TABLE}.aaid ;;
  }

  dimension: advertising_partner_name {
    type: string
    sql: ${TABLE}.advertising_partner_name ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
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

  measure: install_count {
    type: count_distinct
    sql: ${anonymous_id} ;;
  }

  set: detail {
    fields: [
      anonymous_id,
      ip,
      aaid,
      advertising_partner_name,
      campaign,
      timestamp_time
    ]
  }
}
