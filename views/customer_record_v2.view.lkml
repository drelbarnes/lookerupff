view: customer_record_v2 {
  derived_table: {
    sql: with customer_record as (
          select
          id
          , date
          , customer_id
          , subscription_id
          , user_id
          , plan
          , status
          , topic
          , platform
          , frequency
          , last_billed_at
          , next_billing_at
          , payment_method_gateway
          , payment_method_status
          , card_funding_type
          , subscription_due_invoices_count
          , subscription_due_since
          , day_of_dunning
          , total_dues
          , days_at_status
          , total_days_at_status
          , days_on_record
          , total_days_on_record
          from ${customer_record.SQL_TABLE_NAME}
        )
        , bundling as (
          select
            *
            , count(case when status in ('free_trial', 'enabled', 'non_renewing') then subscription_id end) over (partition by customer_id, date) as active_services_count
            , case
              when count(*) over (partition by customer_id, date, next_billing_at) >= 2 then count(*) over (partition by customer_id, date, next_billing_at)
              else 0
            end as bundled_services_count
            , case
              when count(*) over (partition by customer_id, date, next_billing_at) >= 2 then true
              else false
            end as is_bundled
          from customer_record
        )
        , final_output as (
          select
            a.*
            , b.active_services_count
            , b.bundled_services_count
            , b.is_bundled
          from customer_record a
          left join bundling b
          on a.customer_id = b.customer_id
          and a.subscription_id = b.subscription_id
          and a.date = b.date
        )
        select * from final_output
       ;;
    datagroup_trigger: upff_daily_refresh_datagroup
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: date {
    type: time
    sql: ${TABLE}.date ;;
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: plan {
    type: string
    sql: ${TABLE}.plan ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: payment_method_gateway {
    type: string
    sql: ${TABLE}.payment_method_gateway ;;
  }

  dimension: payment_method_status {
    type: string
    sql: ${TABLE}.payment_method_gateway ;;
  }

  dimension: card_funding_type {
    type: string
    sql: ${TABLE}.card_funding_type ;;
  }

  dimension_group: last_billed_at {
    type: time
    sql: ${TABLE}.last_billed_at ;;
  }

  dimension_group: next_billing_at {
    type: time
    sql: ${TABLE}.next_billing_at ;;
  }

  dimension: subscription_due_invoices_count {
    type: number
    sql: ${TABLE}.subscription_due_invoices_count ;;
  }

  dimension_group: subscription_due_since {
    type: time
    sql: ${TABLE}.subscription_due_since ;;
  }

  dimension: day_of_dunning{
    type: number
    sql: ${TABLE}.day_of_dunning ;;
  }

  dimension: services_count{
    type: number
    sql: ${TABLE}.services_count ;;
  }

  dimension: is_bundled{
    type: yesno
    sql: ${TABLE}.is_bundled ;;
  }

  dimension: total_dues {
    type: number
    value_format: "$#.00;($#.00)"
    sql: ${TABLE}.total_dues ;;
  }

  dimension: days_at_status {
    type: number
    sql: ${TABLE}.days_at_status ;;
  }

  dimension: total_days_at_status {
    type: number
    sql: ${TABLE}.total_days_at_status ;;
  }

  dimension: days_on_record {
    type: number
    sql: ${TABLE}.days_on_record ;;
  }

  dimension: total_days_on_record {
    type: number
    sql: ${TABLE}.total_days_on_record ;;
  }

  dimension: row {
    type: number
    primary_key: yes
    sql: ${TABLE}.row ;;
  }

  set: detail {
    fields: [
      date_time,
      user_id,
      status,
      topic,
      platform,
      frequency,
      days_at_status,
      total_days_at_status,
      days_on_record,
      total_days_on_record,
      row
    ]
  }
}
