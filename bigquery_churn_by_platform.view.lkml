view: bigquery_churn_by_platform {
  derived_table: {
    sql: with a as
      (select distinct user_id,
             email,
             platform,
             date(status_date) as status_date
      from http_api.purchase_event
      where topic in ('customer.product.disabled','customer.product.cancelled','customer.product.expired') and date_diff(date(status_date),date(created_at),day)>28),

      b as
      (select distinct user_id,
             email,
             platform,
             date(status_date) as status_date
      from http_api.purchase_event
      where topic in ('customer.product.created','customer.product.renewed')),

      c as
      (select user_id,
             max(status_date) as status_date
      from http_api.purchase_event
      group by 1),

      d as
      (select a.user_id,
             email,
             platform,
             topic,
             plan,
             date(a.status_date) as status_date
      from http_api.purchase_event as a inner join c on a.user_id=c.user_id and a.status_date=c.status_date
      where topic not in ('customer.product.disabled','customer.product.cancelled','customer.product.expired','customer.created','customer.product.charge_failed') and date_diff(current_date(),date(a.status_date),day)>45),

      e as
      (SELECT day
      FROM UNNEST(
          GENERATE_DATE_ARRAY(date_sub(current_date(),interval 1 year), CURRENT_DATE(), INTERVAL 1 month)
      ) AS day),

      f as
      (select user_id,
             email,
             platform,
             date_sub(status_date,interval date_diff(status_date,day,month) month) as status_date
      from d, e
      where status_date>=day),

      g as
      (select * from b
      union all
      select * from f),

      h as
      (select status_date,
             platform,
             count(distinct user_id) as total_paying
      from g
      group by 1,2),

      i as
      (select h.*,
             b.total_paying as monthly_paid,
             b.status_date as status_date2
      from h left join h as b on date_diff(h.status_date,b.status_date,day)<=60 and date_diff(h.status_date,b.status_date,day)>=30 and h.platform=b.platform),

      j as
      (select status_date,
             platform,
             sum(monthly_paid) as paying_30_days
      from i
      group by 1,2),

      k as
      (select status_date,
             platform,
             count(distinct user_id) as total_churn
      from a
      group by 1,2),

      l as
      (select k.*,
             b.total_churn as churn_30_days,
             b.status_date as status_date2
      from k left join k as b on date_diff(k.status_date,b.status_date,day)<=30 and date_diff(k.status_date,b.status_date,day)>=0 and k.platform=b.platform),

      m as
      (select status_date,
             platform,
             sum(churn_30_days) as churn_30_days
      from l
      group by 1,2)

      select j.*,
             churn_30_days
      from j left join m on j.status_date=m.status_date and j.platform=m.platform
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }


  dimension_group: status_date {
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
    sql: safe_cast(${TABLE}.status_date as timestamp) ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: paying_30_days {
    type: number
    sql: ${TABLE}.paying_30_days ;;
  }

  measure: paying_30_days_ {
    type: sum
    sql: ${TABLE}.paying_30_days ;;
  }

  dimension: churn_30_days {
    type: number
    sql: ${TABLE}.churn_30_days ;;
  }

  measure: churn_30_days_ {
    type: sum
    sql: ${TABLE}.churn_30_days ;;
  }

  set: detail {
    fields: [platform, paying_30_days, churn_30_days]
  }
}
