view: gtv_webhook_events {
  derived_table: {
    sql: with vimeo_webhook_events as (
        select timestamp
        , cast(user_id as string) as customer_id
        , concat('GaitherTV-', upper(platform), '-' , cast(user_id as string)) as subscription_id
        , user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date
        ,  concat('GaitherTV-', upper(platform), '-', case when subscription_frequency in (null, "custom", "monthly") then "Monthly" else "Yearly" end) as plan
        , platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site
        , case
          when subscription_frequency in (null, "custom", "monthly") then "monthly"
          else "yearly"
          end as subscription_frequency
          , subscription_price, subscription_status, updated_at, cast(null as int) as event_priority
          , safe_cast(null as string) as payment_method_gateway
          , safe_cast(null as string) as payment_method_status
          , safe_cast(null as string) as card_funding_type
          , safe_cast(null as int) as subscription_due_invoices_count
          , safe_cast(null as timestamp) as subscription_due_date
          , safe_cast(null as timestamp) as subscription_due_since
          , safe_cast(null as int) as subscription_total_dues
          from ${gtv_vimeo_webhook_events.SQL_TABLE_NAME}
      )
      , chargebee_webhook_events as (
      select timestamp, customer_id, subscription_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at, event_priority
        , payment_method_gateway
        , payment_method_status
        , card_funding_type
        , subscription_due_invoices_count
        , subscription_due_date
        , subscription_due_since
        , subscription_total_dues
      from ${upff_chargebee_webhook_events.SQL_TABLE_NAME}
      where (plan like '%Gaither%' and plan is not null)
      )
      , user_ids as (
        select * from ${chargebee_vimeo_ott_id_mapping.SQL_TABLE_NAME} where product_id = "137985"
      )
      , user_id_mapping as (
      select timestamp, a.customer_id, subscription_id, safe_cast(b.ott_user_id as string) as user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at, event_priority
        , payment_method_gateway
        , payment_method_status
        , card_funding_type
        , subscription_due_invoices_count
        , subscription_due_date
        , subscription_due_since
        , subscription_total_dues
      from chargebee_webhook_events a
      left join user_ids b
      on a.customer_id = b.customer_id
      )
      , unionised_purchase_events as (
        select * from vimeo_webhook_events
        union all
        select * from user_id_mapping
      )
      select *
      , row_number() over (order by timestamp, user_id) as row
      , row_number() over (partition by email order by timestamp desc) as user_event_number
      from unionised_purchase_events
    ;;
    datagroup_trigger: upff_daily_refresh_datagroup
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: row {
    primary_key: yes
    type: number
    sql: ${TABLE}.row ;;
  }

  dimension: user_event_number {
    type: number
    sql: ${TABLE}.user_event_number ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
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

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: transformed_event {
    type: string
    sql:
    CASE
      WHEN ${gtv_webhook_events.event} LIKE 'customer_%' THEN
        CASE
          WHEN ${gtv_webhook_events.event} LIKE 'customer_product_%' THEN
            REGEXP_REPLACE(
              ${gtv_webhook_events.event},
              '^customer_product_(.*)',
              'customer.product.\\1'
            )
          ELSE
            REGEXP_REPLACE(
              ${gtv_webhook_events.event},
              '^customer_(.*)',
              'customer.\\1'
            )
        END
      ELSE ${gtv_webhook_events.event}
    END
  ;;
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
    sql: ${TABLE}.country ;;
  }

  dimension_group: created_at {
    type: time
    sql: ${TABLE}.created_at ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension_group: last_payment_date {
    type: time
    sql: ${TABLE}.last_payment_date ;;
  }

  dimension: marketing_opt_in {
    type: yesno
    sql: ${TABLE}.marketing_opt_in ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension_group: next_payment_date {
    type: time
    sql: ${TABLE}.next_payment_date ;;
  }

  dimension: plan {
    type: string
    sql: ${TABLE}.plan ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: promotion_code {
    type: string
    sql: ${TABLE}.promotion_code ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: registered_to_site {
    type: yesno
    sql: ${TABLE}.registered_to_site ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: subscribed_to_site {
    type: yesno
    sql: ${TABLE}.subscribed_to_site ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: subscription_price {
    type: number
    sql: ${TABLE}.subscription_price ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension_group: updated_at {
    type: time
    sql: ${TABLE}.updated_at ;;
  }

  dimension: event_priority {
    type: number
    sql: ${TABLE}.event_priority ;;
  }

  set: detail {
    fields: [
      timestamp_time,
      user_id,
      event,
      campaign,
      city,
      country,
      created_at_time,
      email,
      first_name,
      last_name,
      last_payment_date_time,
      marketing_opt_in,
      name,
      next_payment_date_time,
      plan,
      platform,
      promotion_code,
      referrer,
      region,
      registered_to_site,
      source,
      subscribed_to_site,
      subscription_frequency,
      subscription_price,
      subscription_status,
      updated_at_time,
      row,
      user_event_number
    ]
  }
}
