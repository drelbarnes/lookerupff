view: bigquery_monthly_to_annual_conversions {
  derived_table: {
    sql: with annual as
      (select distinct user_id,
             date(status_date) as annual_status_date
      from http_api.purchase_event
      where subscription_frequency = 'yearly' and topic='customer.product.renewed'),

      monthly as
      (select distinct user_id,
             date(status_date) as monthly_status_date
      from http_api.purchase_event
      where subscription_frequency = 'monthly' and topic='customer.product.renewed'),

      a as
      (select a.user_id,
             monthly_status_date,
             annual_status_date
      from monthly as a left join annual as b on a.user_id=b.user_id and annual_status_date>monthly_status_date),

      b as
      (select user_id,
             annual_status_date,
             max(monthly_status_date) as monthly_status_date
      from a
      group by 1,2),

      c as
      (select FORMAT_TIMESTAMP('%Y-%m', timestamp(monthly_status_date)) as monthly_status_month,
             sum(case when annual_status_date is null then 0 else 1 end) as annual_conversions
      from b
      group by 1),

      d as
      (select FORMAT_TIMESTAMP('%Y-%m', timestamp(monthly_status_date)) as monthly_status_month,
             count(FORMAT_TIMESTAMP('%Y-%m', timestamp(monthly_status_date))) as monthly_subs,
      from a
      group by 1)

      select c.monthly_status_month,
             monthly_subs,
             annual_conversions
      from c inner join d on c.monthly_status_month=d.monthly_status_month
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: monthly_status_month {
    type: string
    sql: ${TABLE}.monthly_status_month ;;
  }

  dimension: monthly_subs {
    type: number
    sql: ${TABLE}.monthly_subs ;;
  }

  dimension: annual_conversions {
    type: number
    sql: ${TABLE}.annual_conversions ;;
  }

  measure: monthly_subs_ {
    type: sum
    sql: ${TABLE}.monthly_subs ;;
  }

  measure: annual_conversions_ {
    type: sum
    sql: ${TABLE}.annual_conversions ;;
  }

  set: detail {
    fields: [monthly_status_month, monthly_subs, annual_conversions]
  }
}
