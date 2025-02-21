view: customer_record_v2 {
  derived_table: {
    sql: with customer_record_p0 as (
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
        , subscription_due_date
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
          , count(case when is_bundled is true then customer_id end) over (partition by customer_id, date) as bundled_services
          , CASE
            WHEN is_bundled is true AND topic = 'customer_product_free_trial_created' and days_at_status = 1 THEN TRUE
            ELSE FALSE
            END AS is_bundle_trial_start
          , case
            -- when is_bundled is true and topic = 'customer_product_free_trial_converted' and days_at_status = 1 then true
            when is_bundled is true and topic = 'customer_product_free_trial_converted' and count(case when is_bundled is true then customer_id end) over (partition by customer_id, date) = count(case when is_bundled is true and status = "enabled" then plan end) over (partition by customer_id, date) and min_days_at_status = 1 then true
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
              , min(days_at_status) over (partition by customer_id, date) as min_days_at_status
            from customer_record_p0
          )
        )
        , bundling_p1 as (
          select *
          , case
            when is_bundle_trial_start is true then "bundle_free_trial_created"
            when is_bundle_trial_converted is true and bundled_services > 0 and bundled_services = count(case when is_bundle_trial_converted then customer_id end) over (partition by customer_id, date) then "bundle_free_trial_converted"
            when is_bundle_trial_expired is true and (bundled_services > 0 and active_services > 0) then "bundle_free_trial_downgraded"
            when is_bundle_trial_expired is true and (bundled_services = 0 and active_services > 0) then "bundle_free_trial_unbundled"
            when is_bundle_trial_expired is true and (previous_bundled_services >= 2 and bundled_services = 0 and active_services = 0) then "bundle_free_trial_expired"
            when is_bundle_trial_converted is true and bundled_services != count(case when is_bundle_trial_converted then customer_id end) over (partition by customer_id, date) and bundled_services > previous_bundled_services then "bundle_upgraded"
            when is_bundle_paying_churn is true and bundled_services != count(case when is_bundle_trial_converted then customer_id end) over (partition by customer_id, date) and bundled_services < previous_bundled_services then "bundle_downgraded"
            when is_bundle_paying_churn is true and previous_bundled_services >= 2 and bundled_services = 0  and active_services > 0 then "bundle_unbundled"
            when is_bundle_paying_churn is true and previous_bundled_services >= 2 and bundled_services = 0 and active_services = 0 then "bundle_paying_churn"
            else safe_cast(null as string)
            end as bundle_topic
          -- , case
          --   when is_bundle_trial_start is true then "free_trial"
          --   when is_bundle_trial_converted is true and bundled_services > 0 and bundled_services = count(case when is_bundle_trial_converted then customer_id end) over (partition by customer_id, date) then "enabled"
          --   when is_bundle_trial_expired is true and (bundled_services > 0 and active_services > 0) then "enabled"
          --   when is_bundle_trial_expired is true and (bundled_services = 0 and active_services > 0) then "expired"
          --   when is_bundle_trial_expired is true and (bundled_services = 0 and previous_bundled_services >= 2) then "expired"
          --   when is_bundle_trial_converted is true and bundled_services > 0 and bundled_services != count(case when is_bundle_trial_converted then customer_id end) over (partition by customer_id, date) and bundled_services > previous_bundled_services then "enabled"
          --   when is_bundle_paying_churn is true and bundled_services > 0 and bundled_services != count(case when is_bundle_trial_converted then customer_id end) over (partition by customer_id, date) and bundled_services < previous_bundled_services then "enabled"
          --   when is_bundle_paying_churn is true and previous_bundled_services >= 2 and bundled_services = 0 then "expired"
          --   else safe_cast(null as string)
          -- end as bundle_status
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
        , bundling_p2 as (
          select a.*
          , coalesce(b.bundled_plan_names, c.bundled_plan_names) as bundled_plan_names
          , case
            when bundle_topic = "bundle_free_trial_created" then "free_trial"
            when bundle_topic in ("bundle_free_trial_converted", "bundle_upgraded", "bundle_free_trial_downgraded", "bundle_downgraded") then "enabled"
            when bundle_topic in ("bundle_free_trial_unbundled","bundle_free_trial_expired", "bundle_unbundled", "bundle_paying_churn") then "expired"
            else safe_cast(null as string)
          end as bundle_status
          from bundling_p1 a
          left join (
            SELECT
            customer_id,
            is_bundled,
            date,
            STRING_AGG(DISTINCT plan ORDER BY plan desc) AS bundled_plan_names,
            FROM bundling_p1
            WHERE is_bundled = TRUE
            GROUP BY customer_id, is_bundled, date
          ) b
          on a.customer_id = b.customer_id and a.date = b.date and a.is_bundled = b.is_bundled
          left join (
          SELECT
          customer_id,
          was_bundled,
          date,
          STRING_AGG(DISTINCT plan ORDER BY plan desc) AS bundled_plan_names,
          FROM bundling_p1
          WHERE was_bundled = TRUE
          GROUP BY customer_id, was_bundled, date
          ) c
          on a.customer_id = c.customer_id and a.date = c.date and a.was_bundled = c.was_bundled
        )
        -- , bundled_plans AS (
        -- SELECT
        -- customer_id,
        -- subscription_id,
        -- date,
        -- STRING_AGG(DISTINCT plan ORDER BY plan desc) AS bundled_plan_names,
        -- FROM bundling_p3
        -- WHERE is_bundled = TRUE
        -- GROUP BY customer_id, subscription_id, date
        -- )
        -- , expired_bundled_plans as (
        -- SELECT
        -- customer_id,
        -- subscription_id,
        -- date,
        -- STRING_AGG(DISTINCT plan ORDER BY plan desc) AS bundled_plan_names,
        -- FROM bundling_p3
        -- WHERE was_bundled = TRUE
        -- GROUP BY customer_id, date
        -- )
        , bundling_p3 as (
          select
          id
          , a.date
          , a.customer_id
          , a.subscription_id
          , user_id
          , plan
          , active_services
          , bundled_services
          , bundled_plan_names
          , is_bundled
          , was_bundled
          , is_bundle_trial_start
          , is_bundle_trial_converted
          , is_bundle_trial_expired
          , is_bundle_paying_churn
          , max(bundle_status) over (partition by a.customer_id, b.bundle_status_group) as bundle_status
          , max(bundle_topic) over (partition by a.customer_id, b.bundle_status_group) as bundle_topic
          , b.bundle_status_group
          from bundling_p2 a
          left join (
            select
            date
            , customer_id
            , subscription_id
            , count(bundle_status) over (partition by customer_id order by date) as bundle_status_group
            from bundling_p2
            where is_bundled is true
            group by date, customer_id, subscription_id, bundle_status, is_bundled
          ) b
          on a.customer_id = b.customer_id and a.subscription_id = b.subscription_id and a.date = b.date
        )
        , bundled_plans AS (
        SELECT
        customer_id,
        date,
        sum(ifnull(bundle_status_group / nullif(bundle_status_group,0),1)) over (partition by customer_id, bundle_status_group order by date) as days_at_bundle_status
        FROM bundling_p3
        WHERE is_bundled = TRUE
        GROUP BY customer_id, date, bundle_status_group
        )
        , expired_bundled_plans as (
        SELECT
        customer_id,
        date,
        sum(ifnull(bundle_status_group / nullif(bundle_status_group,0),1)) over (partition by customer_id, bundle_status_group order by date) as days_at_bundle_status
        FROM bundling_p3
        WHERE was_bundled = TRUE
        GROUP BY customer_id, date, bundle_status_group
        )
        , final_output as (
          select
            a.id
          , a.date
          , a.customer_id
          , a.subscription_id
          , a.user_id
          , a.plan
          , a.status
          , a.topic
          , a.platform
          , a.frequency
          , a.last_billed_at
          , a.next_billing_at
          , a.payment_method_gateway
          , a.payment_method_status
          , a.card_funding_type
          , a.subscription_due_invoices_count
          , a.subscription_due_date
          , a.subscription_due_since
          , a.billing_attempts
          , a.day_of_dunning
          , a.subscription_total_dues
          , a.days_at_status
          , a.total_days_at_status
          , a.days_on_record
          , a.total_days_on_record
          , b.bundled_plan_names
          , b.bundle_topic
          , b.bundle_status
          , coalesce(c.days_at_bundle_status, d.days_at_bundle_status) as days_at_bundle_status
          , b.active_services
          , b.bundled_services
          , b.is_bundled
          , b.was_bundled
          , b.is_bundle_trial_start
          , b.is_bundle_trial_converted
          , b.is_bundle_trial_expired
          , b.is_bundle_paying_churn
          from customer_record_p0 a
          left join bundling_p3 b
          on a.customer_id = b.customer_id
          and a.subscription_id = b.subscription_id
          and a.date = b.date
          LEFT JOIN bundled_plans c
          ON a.customer_id = c.customer_id
          AND a.date = c.date
          left join expired_bundled_plans d
          on a.customer_id = d.customer_id
          and a.date = d.date
          group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
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

  dimension_group: subscription_due_date {
    type: time
    sql: ${TABLE}.subscription_due_date ;;
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

  dimension: bundled_plan_names {
    type: string
    sql: ${TABLE}.bundled_plan_names ;;
  }

  dimension: days_at_bundle_status {
    type: number
    sql: ${TABLE}.days_at_bundle_status ;;
  }

  dimension: bundle_status {
    type: string
    sql: ${TABLE}.bundle_status ;;
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
