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
        , billing_attempts
        , day_of_dunning
        , subscription_total_dues
        , days_at_status
        , total_days_at_status
        , days_on_record
        , total_days_on_record
        from ${customer_record.SQL_TABLE_NAME}
      )
      , bundling_p0 as (
        select *
        , count(case when is_bundled then customer_id end) over (partition by customer_id, date) as bundled_services
        , CASE
          WHEN is_bundled is true AND topic = 'customer_product_free_trial_created' and days_at_status = 1 THEN TRUE
          ELSE FALSE
          END AS is_bundle_trial_start
        , case when is_bundled is true and topic = 'customer_product_free_trial_converted' and days_at_status = 1 then true
          else false
          end as is_bundle_trial_converted
        , lag(is_bundled) over (partition by customer_id, subscription_id order by date) as was_bundled
        , lag(active_services) over (partition by customer_id, subscription_id order by date) as previous_active_services
        from (
          select
            *
            , count(case when status in ('free_trial', 'enabled', 'non_renewing') then subscription_id end) over (partition by customer_id, date) as active_services
            , case
              when next_billing_at is not null and count(*) over (partition by customer_id, date, next_billing_at) >= 2 then true
              else false
            end as is_bundled
          from customer_record
        )
      )
      , bundling_p1 as (
        select *
        , case
          when is_bundle_trial_start and active_services = bundled_services then "bundle_free_trial_created"
          when is_bundle_trial_converted and bundled_services = count(case when is_bundle_trial_converted then customer_id end) over (partition by customer_id, date) then "bundle_free_trial_converted"
          when is_bundle_trial_expired and (bundled_services > 0 or active_services > 0) then "bundle_free_trial_downgraded"
          when is_bundle_trial_expired and previous_bundled_services >= 2 and bundled_services = 0 then "bundle_free_trial_expired"
          when is_bundle_trial_converted and bundled_services != count(case when is_bundle_trial_converted then customer_id end) over (partition by customer_id, date) and bundled_services > previous_bundled_services then "bundle_upgraded"
          when is_bundle_paying_churn and bundled_services != count(case when is_bundle_trial_converted then customer_id end) over (partition by customer_id, date) and bundled_services < previous_bundled_services then "bundle_downgraded"
          when is_bundle_paying_churn and previous_bundled_services >= 2 and bundled_services = 0 then "bundle_paying_churn"
          else safe_cast(null as string)
        end as bundle_topic
        from (
          select *
          , lag(bundled_services) over (partition by customer_id, subscription_id order by date) as previous_bundled_services
          , case when is_bundled is false and was_bundled is true and topic = 'customer_product_free_trial_expired' and days_at_status = 1 then true
            else false
            end as is_bundle_trial_expired
          , case when is_bundled is false and was_bundled is true and topic in ("customer_product_expired", "customer_product_cancelled") and days_at_status = 1 then true
            else false
            end as is_bundle_paying_churn
          from bundling_p0
        )
      )
      , final_output as (
        select
          a.*
          , b.bundle_topic
          , b.active_services
          , b.bundled_services
          , b.is_bundled
          , b.was_bundled
          , b.is_bundle_trial_start
          , b.is_bundle_trial_converted
          , b.is_bundle_trial_expired
          , b.is_bundle_paying_churn
        from customer_record a
        left join bundling_p1 b
        on a.customer_id = b.customer_id
        and a.subscription_id = b.subscription_id
        and a.date = b.date
      )
      select * from final_output
      order by date, plan
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

  dimension: day_of_dunning {
    type: number
    sql: ${TABLE}.day_of_dunning ;;
  }

  dimension: billing_attempts {
    type: number
    sql: ${TABLE}.billing_attempts ;;
  }

  dimension: services_count {
    type: number
    sql: ${TABLE}.services_count ;;
  }

  dimension: is_bundled {
    type: yesno
    sql: ${TABLE}.is_bundled ;;
  }

  dimension: was_bundled {
    type: yesno
    sql: ${TABLE}.was_bundled ;;
  }

  dimension: subscription_total_dues {
    type: number
    value_format: "$#.00;($#.00)"
    sql: ${TABLE}.subscription_total_dues ;;
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

  dimension: bundle_topic {
    type: string
    sql: ${TABLE}.bundle_topic ;;
  }

  dimension: active_services {
    type: number
    sql: ${TABLE}.active_services ;;
  }

  dimension: bundled_services {
    type: number
    sql: ${TABLE}.bundled_services ;;
  }

  dimension: is_bundle_trial_start {
    type: yesno
    sql: ${TABLE}.is_bundle_trial_start ;;
  }

  dimension: is_bundle_trial_converted {
    type: yesno
    sql: ${TABLE}.is_bundle_trial_converted ;;
  }

  dimension: is_bundle_trial_expired {
    type: yesno
    sql: ${TABLE}.is_bundle_trial_expired ;;
  }

  dimension: is_bundle_paying_churn {
    type: yesno
    sql: ${TABLE}.is_bundle_paying_churn ;;
  }

  dimension: id {
    type: string
    primary_key: yes
    sql: ${TABLE}.id ;;
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
      id
    ]
  }
}
