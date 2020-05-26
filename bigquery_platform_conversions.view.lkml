view: bigquery_platform_conversions {
  derived_table: {
    sql: with a as
      (select distinct user_id,
             platform,
             date(created_at) as created_at
      from http_api.purchase_event
      where topic in ('customer.created','customer.product.free_trial_created') and plan <>'none'),

      b as
      (select user_id,
             date(min(status_date)) as status_date
      from http_api.purchase_event
      where (topic in ('customer.product.renewed','customer.product.created') and platform='web') or (platform <>'web' and topic='customer.product.renewed')
      group by 1),

      c as
      (select a.*,
             case when b.user_id is not null then 1 else 0 end as converted
      from a left join b on a.user_id=b.user_id)

      select created_at,
             platform,
             count(distinct user_id) as total_created,
             sum(converted) as total_converted
      from c
      where date_diff(current_date(),created_at,day)>14
      group by 1,2
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: created_at {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      day_of_week_index,
      day_of_week,
      week,
      month,
      quarter,
      year
    ]
    sql: safe_cast(${TABLE}.created_at as timestamp) ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: total_created {
    type: number
    sql: ${TABLE}.total_created ;;
  }

  measure: total_created_ {
    type: sum
    sql: ${TABLE}.total_created ;;
  }

  dimension: total_converted {
    type: number
    sql: ${TABLE}.total_converted ;;
  }

  measure: total_converted_ {
    type: sum
    sql: ${TABLE}.total_converted ;;
  }

  set: detail {
    fields: [platform, total_created, total_converted]
  }
}
