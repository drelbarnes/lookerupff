view: customers {
  sql_table_name: customers.customers ;;

  dimension: customer_id {
    primary_key: yes
    tags: ["user_id"]
    type: number
    sql: ${TABLE}.customer_id ;;
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

  dimension: state_city {
    type: string
    sql: concat(${TABLE}.state, ${TABLE}.city) ;;
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

  dimension: customer_created_at {
    type: date
    sql: ${TABLE}.customer_created_at ;;
  }

  dimension: date_formatted {
    sql: ${customer_created_at} ;;
    html: {{ rendered_value | date: "%b" }} ;;
  }

  dimension: day_of_week {
    sql: ${customer_created_at} ;;
    html: {{ rendered_value | date: "%a" }} ;;
  }

  dimension_group: creation_timestamp {
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


  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: event_created_at {
    type: date
    sql: ${TABLE}.event_created_at ;;
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
    sql: ${TABLE}.event_created_at ;;
    drill_fields: [referrer]
  }

  dimension: current_date{
    type: date
    sql: current_date;;
}

  dimension: days_in_trial{
    description: "Number of days a user is in free trial"
    type: number
    sql:  DATEDIFF('day', ${customer_created_at}::timestamp, ${current_date}::timestamp) ;;
  }


  dimension: subscription_length{
    description: "Number of days a user has been on the service"
    type: number
    sql:  DATEDIFF('day', ${customer_created_at}::timestamp, ${current_date}::timestamp) ;;
  }



  measure: days_churned {
    type: number
    sql:  DATEDIFF('day', ${event_created_at}::timestamp, ${customer_created_at}::timestamp) ;;

  }

  measure: average_days_by {
    type: average
    sql:  DATEDIFF('day', ${customer_created_at}::timestamp, ${event_created_at}::timestamp) ;;
  }

dimension: days_since_created {
  type: number
  sql:  DATEDIFF('day', ${customer_created_at}::date, ${event_created_at}::date);;
}




  dimension: weeks_since_created {
    type: number
    sql:  DATEDIFF('week', ${customer_created_at}::timestamp, ${event_created_at}::timestamp);;
  }

  dimension: months_since_created {
    type: number
    sql:  DATEDIFF('month', ${customer_created_at}::date, ${event_created_at}::date);;
  }


  dimension: LOS {
    type:  number
    label: "Number of Customers By Revenue"
    sql:  ${months_since_created} * 5.99 ;;
  }

  measure: los_by_plaform {
    type: sum
    label: "Number of Customers By Revenue"
    sql: ${LOS};;
  }

  measure: max_days_by {
    type: max
    sql:  DATEDIFF('day', ${customer_created_at}::timestamp, ${event_created_at}::timestamp) ;;
  }
  measure: min_days_by {
    type: min
    sql:  DATEDIFF('day', ${customer_created_at}::timestamp, ${event_created_at}::timestamp) ;;
  }

  measure: average_days_on {
    type: average
    sql:  DATEDIFF('day', ${event_created_at}::timestamp, ${current_date}::timestamp) ;;
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
    drill_fields: [status, platform, customer_id, email, customer_created_at, status,event_created_at,referrer]
  }

  dimension: product_id {
    type: string
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
    drill_fields: [referrer]
  }

  dimension: status_v2 {
    type: string
    sql: case when ${status}= 'cancelled' then 'churn'
              when ${status}= 'disabled' then 'churn'
              when ${status}= 'expired' then 'churn'
              when ${status}= 'paused' then 'churn'
              when ${status}= 'refunded' then 'churn'
              else ${status} end;;
  }

  dimension: State_Location {
    map_layer_name:us_states
    sql: ${state} ;;
  }

  dimension: marketing_opt_in {
    type: string
    sql: ${TABLE}.marketing_opt_in ;;
  }


  measure: count {
    type: count
    drill_fields: [customer_id, product_name, last_name, first_name, email]
  }

  measure: customer_count {
    type: count_distinct
    sql: ${customer_id} ;;
  }

  measure: customers_android {
    type: number
    sql: ${android_users.count}/${count};;
  }

  measure: customers_iOS {
    type: number
    sql: ${ios_users.count}/${count} ;;
  }

  parameter: date_granularity {
    type: string
    allowed_value: { value: "Day" }
    allowed_value: { value: "Week"}
    allowed_value: { value: "Month" }
    allowed_value: { value: "Quarter" }
    allowed_value: { value: "Year" }
  }

  dimension: date {
    label_from_parameter: date_granularity
    sql:
       CASE
         WHEN {% parameter date_granularity %} = 'Day' THEN
           ${timestamp_date}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Week' THEN
           ${timestamp_week}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Month' THEN
           ${timestamp_month}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Quarter' THEN
           ${timestamp_quarter}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Year' THEN
           ${timestamp_year}::VARCHAR
         ELSE
           NULL
       END ;;
  }

  dimension: days_since_creation{
    type: string
    sql:
      case
        when ${days_since_created}<=14 then '0-14 Days'
        when ${days_since_created}>14 and ${days_since_created}<=21 then '15-21 Days'
        when ${days_since_created}>21 and ${days_since_created}<=28 then '22-28 Days'
        when ${days_since_created}>28 and ${days_since_created}<=35 then '29-35 Days'
        when ${days_since_created}>35 and ${days_since_created}<=49 then '36-49 Days'
        when ${days_since_created}>49 and ${days_since_created}<=56 then '50-56 Days'
        else '56+ Days'
        end;;
  }

  dimension: revenue {
    type: number
    sql:
      case
          when ${status}='enabled' and ${days_since_created}=15 and ${platform}='android' then .7*5.99
          when ${status}='enabled' and ${days_since_created}=15 and ${platform}='android_tv' then .7*5.99
          when ${status}='enabled' and ${days_since_created}=15 and ${platform}='ios' then .7*5.99
          when ${status}='enabled' and ${days_since_created}=15 and ${platform}='tvos' then .7*5.99
          when ${status}='enabled' and ${days_since_created}=15 and ${platform}='roku' then .8*5.99
          when ${status}='enabled' and ${days_since_created}=15 and ${platform}='web' then 5.99
          when ${status}='enabled' and ${days_since_created}>15 and (${days_since_created}-15)%30=0 and ${platform}='android' then .7*5.99
          when ${status}='enabled' and ${days_since_created}>15 and (${days_since_created}-15)%30=0 and ${platform}='android_tv' then .7*5.99
          when ${status}='enabled' and ${days_since_created}>15 and (${days_since_created}-15)%30=0 and ${platform}='ios' then .7*5.99
          when ${status}='enabled' and ${days_since_created}>15 and (${days_since_created}-15)%30=0 and ${platform}='tvos' then .7*5.99
          when ${status}='enabled' and ${days_since_created}>15 and (${days_since_created}-15)%30=0 and ${platform}='roku' then .8*5.99
          when ${status}='enabled' and ${days_since_created}>15 and (${days_since_created}-15)%30=0 and ${platform}='web' then 5.99
        else null end
    ;;
  }


#Get Status by case
  dimension: get_status {
    type:  number
    sql:
      case
        when ${status}='enabled' then 1
        when ${status}='cancelled' AND ${days_since_created} < 15 then 0
      else null end
    ;;
  }


#Get1 Free Trial Cancelled
  dimension: cancelled_during_free_trial {
    case: {
      when: {
        sql: ${days_since_created} = 14;;
        label: "Free Trial Cancelled"
      }
    }
  }


#Get Marketing Opt-In by case
  dimension: get_marketing_opt_in {
    type:  number
    sql:
      case
        when ${marketing_opt_in}='Y' then 1
        when ${marketing_opt_in}='N' then 0
      else null end
    ;;
  }

  measure: revenue_ {
    type: sum
    sql: ${revenue} ;;
  }
}
