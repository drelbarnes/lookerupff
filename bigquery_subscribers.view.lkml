view: bigquery_subscribers {
  sql_table_name: customers.subscribers ;;

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
    tags: ["email"]
    sql:  ${TABLE}.email ;;
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
    sql: SAFE_CAST(${TABLE}.marketing_opt_in AS int64);;
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

  measure: count {
    type: count
    drill_fields: [last_name, product_name, first_name]
  }


  #------------------------ New Dimensions 9/23

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

  dimension: LOS {
    type:  number
    label: "Revenue By Subscriber"
    sql:  ${months_since_created} * 5.99 ;;
  }

  dimension: state_city {
    type: string
    sql: concat(${TABLE}.state, ${TABLE}.city) ;;
  }

  dimension: subscription_length{
    description: "Number of days a user has been on the service"
    type: number
    sql:  DATE_DIFF(${current_date}, ${customer_created_date}, DAY) ;;
  }

  dimension: days_since_created {
    type: number
    sql:  DATE_DIFF(${event_created_date}, ${customer_created_date}, DAY);;
  }

  dimension: months_since_created {
    type: number
    sql:  DATE_DIFF(${event_created_date}, ${customer_created_date}, MONTH);;
  }

  dimension: date_formatted {
    sql: ${customer_created_date} ;;
    html: {{ rendered_value | date: "%b" }} ;;
  }

  dimension: day_of_week {
    sql: ${customer_created_date} ;;
    html: {{ rendered_value | date: "%a" }} ;;
  }

  dimension: days_in_trial{
    description: "Number of days a user is in free trial"
    type: number
    sql:  DATE_DIFF(${current_date}, ${bigquery_subscribers.customer_created_date}, DAY) ;;
  }

  measure: timecode {
    type: number
    value_format: "0"
    sql: ${bigquery_derived_timeupdate.timecode_count};;
  }

  dimension: timecode_count {
    type: number
    sql:  ${bigquery_subscribers_timeupdate.timecode_count};;
  }

  dimension: addwatchlist {
    type:  string
    sql: ${bigquery_derived_addwatchlist.event};;
  }

  measure: addwatchlist_count {
    type: number
    sql:count(${addwatchlist});;
  }

  dimension: signgin {
    type:  string
    sql: ${bigquery_derived_signin.event};;
  }

  measure: signin_count {
    type: number
    sql: COUNT(${signgin});;
  }

  dimension: views {
    type:  string
    sql: ${bigquery_derived_views.user_id};;
  }

  measure: views_count {
    type: number
    sql: COUNT(${views});;
  }

  measure: count_distinct {
    type: count_distinct
    sql: ${email};;
  }

  measure: average_tenure{
    type:  average
    sql:  ${months_since_created};;
  }

  #------------------------ End New Dimensions
}
