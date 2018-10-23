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
(select user_id, count(1) as number_of_platforms from a group by 1 order by 2 desc)

select s.*,
       number_of_platforms
from customers.subscribers as s inner join b on s.customer_id=safe_cast(user_id as int64) ;;
    }

    dimension: number_of_platforms {
      type: number
      sql: ${TABLE}.number_of_platforms/3 ;;
    }

    dimension: action {
      type: string
      sql: ${TABLE}.action ;;
    }

    dimension: action_type {
      type: string
      sql: ${TABLE}.action_type ;;
    }

    dimension: campaign {
      type: string
      sql: ${TABLE}.campaign ;;
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

    dimension_group: customer_created {
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
      sql: ${TABLE}.customer_created_at ;;
    }

    dimension: customer_id {
      type: number
      primary_key: yes
      tags: ["user_id"]
      sql: ${TABLE}.customer_id ;;
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

    dimension: marketing_opt_in {
      type: number
      sql: ${TABLE}.marketing_opt_in ;;
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
      sql: ${TABLE}.state ;;
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
      sql:
      case
        when ${status}='enabled' then 1
        when ${status} in ('cancelled', 'disabled','expired','refunded') AND ${days_since_created} < 15 then 0
      else null end
    ;;
    }

    dimension: subscription_length{
      description: "Number of days a user has been on the service"
      type: number
      sql:  DATE_DIFF(${current_date}, ${customer_created_date}, DAY) ;;
    }

    dimension: day_of_week {
      type: date_day_of_week
      sql: ${TABLE}.customer_created_at ;;
    }

    measure: count {
      type: count
      drill_fields: [last_name, product_name, first_name]
    }
    }
