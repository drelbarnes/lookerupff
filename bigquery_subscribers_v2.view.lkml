  view: bigquery_subscribers_v2 {
    derived_table: {
      sql:
with a as

(select distinct id as user_id, 'web' as source from javascript.users
union all
select distinct id as user_id, 'android' as source from android.users
union all
select distinct id as user_id, 'ios' as source from ios.users),

b as
(select user_id, count(1) as number_of_platforms from a group by 1 order by 2 desc),

purchase_event as
(with
b as
(select user_id, min(received_at) as received_at
from http_api.purchase_event
where topic in ('customer.product.free_trial_created','customer.product.created','customer.created') and date(created_at)=date(received_at) and date(created_at)>'2018-10-31'
group by 1)

select a.user_id, a.platform, created_at, region
from b inner join http_api.purchase_event as a on a.user_id=b.user_id and a.received_at=b.received_at
where topic in ('customer.product.free_trial_created','customer.product.created','customer.created') and date(created_at)=date(a.received_at) and date(created_at)>'2018-10-31'),

renewed as
(select distinct user_id, 1 as get_status
from http_api.purchase_event
where ((topic='customer.product.renewed' or status='renewed') and date(created_at)>'2018-10-31') or (topic='customer.product.created' and date_diff(date(status_date),date(created_at),day)>14)and date(created_at)>'2018-10-31')

select s.*,
       case when number_of_platforms is null then 1 else number_of_platforms end as number_of_platforms,
       case when get_status is null then 0 else get_status end as get_status
from purchase_event as s left join b on s.user_id=b.user_id left join renewed as c on s.user_id=c.user_id ;;
    }

    dimension: number_of_platforms {
      type: number
      sql: ${TABLE}.number_of_platforms/3 ;;
    }

    dimension: city {
      type: string
      sql: ${TABLE}.city ;;
    }

    dimension: country {
      type: string
      map_layer_name: countries
      sql: ${TABLE}.country ;;
    }

    dimension: coupon_code {
      type: string
      sql: ${TABLE}.coupon_code ;;
    }

    dimension: coupon_code_id {
      type: string
      sql: ${TABLE}.coupon_code_id ;;
    }

    dimension: current_date{
      type: date
      sql: current_date;;
    }

    dimension_group: customer_created{
      type: time
      timeframes: [
        raw,
        day_of_month,
        time,
        date,
        week,
        month,
        quarter,
        year
      ]
      sql: ${TABLE}.created_at ;;
    }

    dimension: customer_created_at_day {
      type: string
      sql: case when extract(DAY from ${TABLE}.created_at) between 1 and 10 then "Beginning"
                when extract(DAY from ${TABLE}.created_at) between 11 and 20 then "Middle"
                when extract(DAY from ${TABLE}.created_at)>20 then "End" end;;
    }

    dimension: user_id {
      type: number
      primary_key: yes
      tags: ["user_id"]
      sql: ${TABLE}.user_id ;;
    }

    dimension: email {
      type: string
      sql: ${TABLE}.email ;;
    }

    dimension_group: event_created {
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
      sql: ${TABLE}.event_created_at ;;
    }

    dimension: first_name {
      type: string
      sql: ${TABLE}.first_name ;;
    }

    dimension: frequency {
      type: string
      sql: ${TABLE}.frequency ;;
    }

    dimension: last_name {
      type: string
      sql: ${TABLE}.last_name ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: product_id {
      type: number
      sql: ${TABLE}.product_id ;;
    }

    dimension: product_name {
      type: string
      sql: ${TABLE}.product_name ;;
    }

    dimension: promotion_code {
      type: string
      sql: ${TABLE}.promotion_code ;;
    }

    dimension: promotion_id {
      type: number
      sql: ${TABLE}.promotion_id ;;
    }

    dimension: referrer {
      type: string
      sql: ${TABLE}.referrer ;;
    }

    dimension: state {
      type: string
      sql: ${TABLE}.region ;;
    }

    dimension: status {
      type: string
      sql: ${TABLE}.status ;;
    }

    dimension: days_since_created {
      type: number
      sql:  DATE_DIFF(${event_created_date}, ${customer_created_date}, DAY);;
    }

#Get Status by case
    dimension: get_status {
      type:  number
      sql:${TABLE}.get_status
    ;;
    }

    dimension: subscription_length{
      description: "Number of days a user has been on the service"
      type: number
      sql:  DATE_DIFF(${current_date}, ${customer_created_date}, DAY) ;;
    }

    dimension: day_of_week {
      type: date_day_of_week
      sql: ${TABLE}.created_at ;;
    }

    measure: count {
      type: count
      drill_fields: [last_name, product_name, first_name]
    }
    }
