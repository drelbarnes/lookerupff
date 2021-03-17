view: bigquery_email_churn {
  derived_table: {
    sql: with aa as
      ((select anonymous_id,
             context_ip,
             referrer,
             date(timestamp) as timestamp,
             split(split(referrer,"utm_campaign=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm
      from javascript_upff_home.pages
      where date(timestamp)>'2019-09-15')
      union all
      (select anonymous_id,
             context_ip,
             referrer,
             date(timestamp) as timestamp,
             split(split(referrer,"utm_campaign=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm
      from javascript.view
      where date(timestamp)>'2019-09-15')),

      a as
      (select distinct * from aa),

      c as
      (((select (a.timestamp) as timestamp, context_traits_email as email, utm from android.users as a1 inner join a on context_traits_anonymous_id=anonymous_id)
      union all
      (select   (a.timestamp) as timestamp, context_traits_email as email, utm from ios.users as a1 inner join a on context_traits_anonymous_id=anonymous_id))
      union all
      (select   (a.timestamp) as timestamp, email, utm from javascript.users as a1 inner join a on a1.context_ip=a.context_ip)),

      d as
      (select distinct timestamp as email_date,
                       email,
                       utm
       from c
       where utm is not null),

      e as
      (select distinct d.*,
             date(status_date) as status_date
      from d inner join http_api.purchase_event as b on d.email=b.email and email_date<=date(status_date)
      where topic in ('customer.product.disabled','customer.product.expired','customer.product.cancelled') and date_diff(date(status_date),date(created_at),day)>28)

      select utm,
             status_date,
             count(distinct email) as churns
      from e
      group by 1,2
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: utm {
    type: string
    sql: ${TABLE}.utm ;;
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
    sql: ${TABLE}.status_date ;;
  }

  dimension: churns {
    type: number
    sql: ${TABLE}.churns ;;
  }

  measure: churns_ {
    type: sum
    sql: ${TABLE}.churns ;;
  }

  set: detail {
    fields: [utm, status_date_date, churns]
  }
}
