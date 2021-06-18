view: annual_kpis {
  derived_table: {
    sql:
       /*Segment trial starts by monthly and yearly plans*/
    with a as
      (select date(created_at) as created_at,
             count(distinct case when subscription_frequency='monthly' then user_id else null end) as monthly_starts,
             count(distinct case when subscription_frequency='yearly' then user_id else null end) as yearly_starts
      from http_api.purchase_event
      where subscription_frequency in ('monthly','yearly') and date(status_date)=date(created_at)
      group by 1),
/*Aggregate both monthly and yearly starts on date*/
      b as
      (select date(status_date) as status_date,
             count(distinct case when subscription_frequency='monthly' then user_id else null end) as monthly_conversions,
             count(distinct case when subscription_frequency='yearly'  then user_id else null end) as yearly_conversions
      from http_api.purchase_event
      where subscription_frequency in ('monthly','yearly') and topic='customer.product.free_trial_converted'
      group by 1)

      select status_date,
             monthly_starts,
             monthly_conversions,
             yearly_starts,
             yearly_conversions
      from a, b
      where date_diff(status_date,a.created_at,day) = 14
      order by 1 desc
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
      week,
      month,
      quarter,
      year
    ]
    sql: timestamp(${TABLE}.status_date) ;;
  }

  dimension: monthly_starts {
    type: number
    sql: ${TABLE}.monthly_starts ;;
  }

  dimension: monthly_conversions {
    type: number
    sql: ${TABLE}.monthly_conversions ;;
  }

  dimension: yearly_starts {
    type: number
    sql: ${TABLE}.yearly_starts ;;
  }

  dimension: yearly_conversions {
    type: number
    sql: ${TABLE}.yearly_conversions ;;
  }

  measure: monthly_starts_ {
    type: sum
    sql: ${TABLE}.monthly_starts ;;
  }

  measure: monthly_conversions_ {
    type: sum
    sql: ${TABLE}.monthly_conversions ;;
  }

  measure: yearly_starts_ {
    type: sum
    sql: ${TABLE}.yearly_starts ;;
  }

 measure: yearly_conversions_ {
    type: sum
    sql: ${TABLE}.yearly_conversions ;;
  }

  set: detail {
    fields: [monthly_starts, monthly_conversions, yearly_starts, yearly_conversions]
  }
}
