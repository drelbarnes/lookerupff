view: customer_record_bundle_analytics {
  derived_table: {
    sql: with customer_record as (
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
      , a.bundled_plan_names
      , a.bundle_topic
      , a.bundle_status
      , a.days_at_bundle_status
      , a.active_services
      , a.bundled_services
      , a.is_bundled
      , a.was_bundled
      , a.is_bundle_trial_start
      , a.is_bundle_trial_converted
      , a.is_bundle_trial_expired
      , a.is_bundle_paying_churn
      from ${customer_record_v2.SQL_TABLE_NAME} a
      -- inner join (select uploaded_at, subscription_id from `up-faith-and-family-216419.chargebee.subscriptions` where subscription_deleted is false) b
      -- on a.subscription_id = b.subscription_id and date(a.date) = date(b.uploaded_at)
      )
      , bundle_free_trial_created_counts as (
        with p0 as (
          select
          a.date
          , a.customer_id
          , a.platform
          , a.frequency
          , a.bundled_plan_names
          , a.bundle_topic
          from (select * from customer_record where bundle_topic = "bundle_free_trial_created" and days_at_bundle_status = 1) a
          group by 1,2,3,4,5,6
        )
        select
        date
        , platform
        , frequency
        , bundled_plan_names
        , count(case when bundle_topic = "bundle_free_trial_created" then bundle_topic end) as bundle_free_trial_created
        from p0
        where platform = "web"
        group by 1,2,3,4
      )
      , bundle_free_trial_converted_counts as (
        with p0 as (
          select
          a.date
          , a.customer_id
          , a.platform
          , a.frequency
          , a.bundled_plan_names
          , b.bundle_topic
          from customer_record a
          left join (select * from customer_record where bundle_topic = "bundle_free_trial_converted" and days_at_bundle_status = 1) b
          on date(a.date) = date(b.subscription_due_date) and a.customer_id = b.customer_id and a.subscription_id = b.subscription_id
          group by 1,2,3,4,5,6
        )
        select
        date
        , platform
        , frequency
        , bundled_plan_names
        , count(case when bundle_topic = "bundle_free_trial_converted" then bundle_topic end) as bundle_free_trial_converted
        from p0
        where platform = "web"
        group by 1,2,3,4
      )
      , bundle_free_trial_expired_counts as (
        with p0 as (
          select
          a.date
          , a.customer_id
          , a.platform
          , a.frequency
          , a.bundled_plan_names
          , b.bundle_topic
          from customer_record a
          left join (select * from customer_record where bundle_topic = "bundle_free_trial_expired" and days_at_bundle_status = 1) b
          on date(a.date) = date(b.subscription_due_date) and a.customer_id = b.customer_id and a.subscription_id = b.subscription_id
          group by 1,2,3,4,5,6
        )
        select
        date
        , platform
        , frequency
        , bundled_plan_names
        , count(case when bundle_topic = "bundle_free_trial_expired" then bundle_topic end) as bundle_free_trial_expired
        from p0
        where platform = "web"
        group by 1,2,3,4
      )
      , bundle_free_trial_downgraded_counts as (
        with p0 as (
          select
          a.date
          , a.customer_id
          , a.platform
          , a.frequency
          , a.bundled_plan_names
          , b.bundle_topic
          from customer_record a
          left join (select * from customer_record where bundle_topic = "bundle_free_trial_downgraded" and days_at_bundle_status = 1) b
          on date(a.date) = date(b.subscription_due_date) and a.customer_id = b.customer_id and a.subscription_id = b.subscription_id
          group by 1,2,3,4,5,6
        )
        select
        date
        , platform
        , frequency
        , bundled_plan_names
        , count(case when bundle_topic = "bundle_free_trial_downgraded" then bundle_topic end) as bundle_free_trial_downgraded
        from p0
        where platform = "web"
        group by 1,2,3,4
      )
      , bundle_counts as (
        with p0 as (
          select
          a.date
          , a.customer_id
          , a.platform
          , a.frequency
          , a.days_at_status
          , a.bundled_plan_names
          , a.bundle_status
          , a.bundle_topic
          , a.is_bundled
          , a.subscription_due_since
          from (select * from customer_record where platform = "web" and is_bundled is true) a
          group by 1,2,3,4,5,6,7,8,9,10
        )
        select
        date
        , platform
        , frequency
        , bundled_plan_names
        , count(case when bundle_status in ('enabled') and is_bundled is true then customer_id end) as total_paying_bundles
        , count(case when bundle_status in ('free_trial') and is_bundled is true and subscription_due_since is null and days_at_status < 8 then customer_id end) as total_free_trial_bundles
        , count(case when bundle_status in ('free_trial') and is_bundled is true and subscription_due_since is not null and days_at_status < 22 then customer_id end) as total_free_trial_bundles_in_dunning
        from p0
        where platform = "web"
        group by 1,2,3,4
      )
      , bundle_downgraded_counts as (
        with p0 as (
          select
          a.date
          , a.customer_id
          , a.platform
          , a.frequency
          , a.bundled_plan_names
          , b.bundle_topic
          from customer_record a
          left join (select * from customer_record where bundle_topic = "bundle_downgraded" and days_at_bundle_status = 1) b
          on date(a.date) = date(b.subscription_due_date) and a.customer_id = b.customer_id and a.subscription_id = b.subscription_id
          group by 1,2,3,4,5,6
        )
        select
        date
        , platform
        , frequency
        , bundled_plan_names
        , count(case when bundle_topic = "bundle_downgraded" then bundle_topic end) as bundle_downgraded
        from p0
        where platform = "web"
        group by 1,2,3,4
      )
      , bundle_upgraded_counts as (
        with p0 as (
          select
          a.date
          , a.customer_id
          , a.platform
          , a.frequency
          , a.bundled_plan_names
          , b.bundle_topic
          from customer_record a
          left join (select * from customer_record where bundle_topic = "bundle_upgraded" and days_at_bundle_status = 1) b
          on date(a.date) = date(b.subscription_due_date) and a.customer_id = b.customer_id and a.subscription_id = b.subscription_id
          group by 1,2,3,4,5,6
        )
        select
        date
        , platform
        , frequency
        , bundled_plan_names
        , count(case when bundle_topic = "bundle_upgraded" then bundle_topic end) as bundle_upgraded
        from p0
        where platform = "web"
        group by 1,2,3,4
      )
      , bundle_paying_churn_counts as (
        with p0 as (
          select
          a.date
          , a.customer_id
          , a.platform
          , a.frequency
          , a.bundled_plan_names
          , b.bundle_topic
          from customer_record a
          left join (select * from customer_record where bundle_topic = "bundle_paying_churn" and days_at_bundle_status = 1) b
          on date(a.date) = date(b.subscription_due_date) and a.customer_id = b.customer_id and a.subscription_id = b.subscription_id
          group by 1,2,3,4,5,6
        )
        select
        date
        , platform
        , frequency
        , bundled_plan_names
        , count(case when bundle_topic = "bundle_paying_churn" then bundle_topic end) as bundle_paying_churn
        from p0
        where platform = "web"
        group by 1,2,3,4
      )
      , bundle_dimensions as (
        select
        a.date
        , a.platform
        , a.frequency
        , a.bundled_plan_names
        , a.total_paying_bundles
        , a.total_free_trial_bundles
        , a.total_free_trial_bundles_in_dunning
        , b.bundle_free_trial_created
        , c.bundle_free_trial_converted
        , d.bundle_free_trial_expired
        , e.bundle_free_trial_downgraded
        , f.bundle_downgraded
        , g.bundle_upgraded
        , h.bundle_paying_churn
        from bundle_counts a
        left join (select * from bundle_free_trial_created_counts where bundled_plan_names is not null) b
        on a.date = b.date and a.frequency= b.frequency and a.bundled_plan_names = b.bundled_plan_names
        left join (select * from bundle_free_trial_converted_counts where bundled_plan_names is not null) c
        on a.date = c.date and a.frequency= c.frequency and a.bundled_plan_names = c.bundled_plan_names
        left join (select * from bundle_free_trial_expired_counts where bundled_plan_names is not null) d
        on a.date = d.date and a.frequency= d.frequency and a.bundled_plan_names = d.bundled_plan_names
        left join (select * from bundle_free_trial_downgraded_counts where bundled_plan_names is not null) e
        on a.date = e.date and a.frequency= e.frequency and a.bundled_plan_names = e.bundled_plan_names
        left join (select * from bundle_downgraded_counts where bundled_plan_names is not null) f
        on a.date = f.date and a.frequency= f.frequency and a.bundled_plan_names = f.bundled_plan_names
        left join (select * from bundle_upgraded_counts where bundled_plan_names is not null) g
        on a.date = g.date and a.frequency= g.frequency and a.bundled_plan_names = g.bundled_plan_names
        left join (select * from bundle_paying_churn_counts where bundled_plan_names is not null) h
        on a.date = h.date and a.frequency= h.frequency and a.bundled_plan_names = h.bundled_plan_names
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

      )
      , bundle_metrics as (
        with bundle_summation as (
          select *
          , sum(bundle_free_trial_created) over (partition by platform, bundled_plan_names, frequency order by date rows between 27 preceding and 14 preceding) as bundle_free_trial_created_14_day_sum_offset
          , sum(bundle_free_trial_converted) over (partition by platform, bundled_plan_names, frequency order by date rows between 13 preceding and current row) as bundle_free_trial_converted_14_day_sum
          , sum(bundle_free_trial_created) over (partition by platform, bundled_plan_names, frequency order by date rows between 13 preceding and 7 preceding) as bundle_free_trial_created_7_day_sum_offset
          , sum(bundle_free_trial_converted) over (partition by platform, bundled_plan_names, frequency order by date rows between 6 preceding and current row) as bundle_free_trial_converted_7_day_sum
          , lag(case when frequency ="monthly" then total_paying_bundles end, 30) over (partition by platform, bundled_plan_names, frequency order by date) as total_monthly_paying_bundles_30_days_prior
          , lag(case when frequency ="yearly" then total_paying_bundles end, 365) over (partition by platform, bundled_plan_names, frequency order by date) as total_yearly_paying_bundles_365_days_prior
          , lag(total_paying_bundles, 30) over (partition by platform, bundled_plan_names, frequency order by date) as total_paying_bundles_30_days_prior
          , sum(case when frequency = "monthly" then bundle_paying_churn end) over (partition by platform, bundled_plan_names, frequency order by date rows between 29 preceding and current row) as monthly_paying_bundle_churn_30_day_sum
          , sum(case when frequency = "yearly" then bundle_paying_churn end) over (partition by platform, bundled_plan_names, frequency order by date rows between 364 preceding and current row) as yearly_paying_bundle_churn_365_day_sum
          , sum(bundle_paying_churn) over (partition by platform, bundled_plan_names, frequency order by date rows between 29 preceding and current row) as paying_bundle_churn_30_day_sum
          from bundle_dimensions
        )
        , bundle_churn_rates as (
          select *
          , ifnull(bundle_free_trial_converted_14_day_sum/nullif(bundle_free_trial_created_14_day_sum_offset,0),null) as bundle_free_trial_14_conversion_rate
          , ifnull(bundle_free_trial_converted_7_day_sum/nullif(bundle_free_trial_created_7_day_sum_offset,0),null) as bundle_free_trial_conversion_rate
          , ifnull(monthly_paying_bundle_churn_30_day_sum / nullif(total_monthly_paying_bundles_30_days_prior,0),null) as bundle_monthly_churn_rate
          , ifnull(yearly_paying_bundle_churn_365_day_sum / nullif(total_yearly_paying_bundles_365_days_prior,0),null) as bundle_yearly_churn_rate
          , ifnull(paying_bundle_churn_30_day_sum / nullif(total_paying_bundles_30_days_prior,0),null) as bundle_paying_churn_rate
          from bundle_summation
        )
        select *
        , avg(bundle_paying_churn_rate) over (partition by platform, bundled_plan_names order by `date` desc rows between 29 preceding and current row) as bundle_platform_churn_rate
        , avg(bundle_paying_churn_rate) over (order by `date` desc rows between 29 preceding and current row) as bundle_global_churn_rate
        from bundle_churn_rates
      )
      , period as (
        select *
        from bundle_metrics
        where
        `date` >= {% date_start date_filter %}
        and `date` <= {% date_end date_filter %}
      )
      select *, row_number() over (order by date) as row from period
       ;;
  }

  filter: date_filter {
    label: "Date Range"
    type: date
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: date {
    type: time
    sql: ${TABLE}.date ;;
  }

  dimension: row {
    type: number
    primary_key: yes
    sql: ${TABLE}.row ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: bundled_plan_names {
    type: string
    sql: ${TABLE}.bundled_plan_names ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: bundle_free_trial_created {
    type: number
    sql: ${TABLE}.bundle_free_trial_created ;;
  }

  measure: bundle_free_trial_created_ {
    label: "bundle_free_trial_created"
    type: sum
    sql: ${bundle_free_trial_created} ;;
  }

  # dimension: free_trial_churn {
  #   type: number
  #   sql: ${TABLE}.free_trial_churn ;;
  # }

  dimension: bundle_free_trial_converted {
    type: number
    sql: ${TABLE}.bundle_free_trial_converted ;;
  }

  # dimension: paying_created {
  #   type: number
  #   sql: ${TABLE}.paying_created ;;
  # }

  dimension: bundle_paying_churn {
    type: number
    sql: ${TABLE}.bundle_paying_churn ;;
  }

  # dimension: paused_created {
  #   type: number
  #   sql: ${TABLE}.paused_created ;;
  # }

  dimension: total_paying_bundles {
    type: number
    sql: ${TABLE}.total_paying_bundles ;;
  }

  dimension: total_free_trial_bundles {
    type: number
    sql: ${TABLE}.total_free_trial_bundles ;;
  }

  dimension: bundle_free_trial_created_14_day_sum_offset {
    type: number
    sql: ${TABLE}.bundle_free_trial_created_14_day_sum_offset ;;
  }

  dimension: bundle_free_trial_converted_14_day_sum {
    type: number
    sql: ${TABLE}.bundle_free_trial_converted_14_day_sum ;;
  }

  dimension: bundle_free_trial_created_7_day_sum_offset {
    type: number
    sql: ${TABLE}.bundle_free_trial_created_7_day_sum_offset ;;
  }

  dimension: bundle_free_trial_converted_7_day_sum {
    type: number
    sql: ${TABLE}.bundle_free_trial_converted_7_day_sum ;;
  }

  dimension: total_monthly_paying_bundles_30_days_prior {
    type: number
    sql: ${TABLE}.total_monthly_paying_bundles_30_days_prior ;;
  }

  dimension: total_yearly_paying_bundles_365_days_prior {
    type: number
    sql: ${TABLE}.total_yearly_paying_bundles_365_days_prior ;;
  }

  dimension: total_paying_30_days_prior {
    type: number
    sql: ${TABLE}.total_paying_30_days_prior ;;
  }

  dimension: monthly_paying_churn_30_day_sum {
    type: number
    sql: ${TABLE}.monthly_paying_churn_30_day_sum ;;
  }

  dimension: yearly_paying_churn_365_day_sum {
    type: number
    sql: ${TABLE}.yearly_paying_churn_365_day_sum ;;
  }

  dimension: paying_churn_30_day_sum {
    type: number
    sql: ${TABLE}.paying_churn_30_day_sum ;;
  }

  dimension: bundle_free_trial_14_conversion_rate {
    type: number
    sql: ${TABLE}.bundle_free_trial_14_conversion_rate ;;
  }

  dimension: bundle_free_trial_conversion_rate {
    type: number
    sql: ${TABLE}.bundle_free_trial_conversion_rate ;;
  }

  dimension: bundle_monthly_churn_rate {
    type: number
    sql: ${TABLE}.bundle_monthly_churn_rate ;;
  }

  dimension: bundle_yearly_churn_rate {
    type: number
    sql: ${TABLE}.bundle_yearly_churn_rate ;;
  }

  dimension: bundle_paying_churn_rate {
    type: number
    sql: ${TABLE}.bundle_paying_churn_rate ;;
  }

  dimension: bundle_platform_churn_rate {
    type: number
    sql: ${TABLE}.bundle_platform_churn_rate ;;
  }

  dimension: bundle_global_churn_rate {
    type: number
    sql: ${TABLE}.bundle_global_churn_rate ;;
  }

  measure: total_paying_churn_30_day_sum {
    type: sum
    sql: ${TABLE}.paying_bundle_churn_30_day_sum ;;
  }

  measure: total_paying_30_days_prior_sum {
    type: sum
    sql: ${TABLE}.total_paying_bundles_30_days_prior ;;
  }

  measure: total_bundle_free_trial_created_7_day_sum_offset {
    type: sum
    sql: ${TABLE}.bundle_free_trial_created_7_day_sum_offset ;;
  }

  measure: total_bundle_free_trial_converted_7_day_sum {
    type: sum
    sql: ${TABLE}.bundle_free_trial_converted_7_day_sum ;;
  }

  set: detail {
    fields: [
      date_time,
      platform,
      frequency,
      bundle_free_trial_created,
      bundle_free_trial_converted,
      bundle_paying_churn,
      total_paying_bundles,
      total_free_trial_bundles,
      bundle_free_trial_created_14_day_sum_offset,
      bundle_free_trial_converted_14_day_sum,
      total_monthly_paying_bundles_30_days_prior,
      total_yearly_paying_bundles_365_days_prior,
      total_paying_30_days_prior,
      monthly_paying_churn_30_day_sum,
      yearly_paying_churn_365_day_sum,
      paying_churn_30_day_sum,
      bundle_free_trial_14_conversion_rate,
      bundle_free_trial_conversion_rate,
      bundle_monthly_churn_rate,
      bundle_yearly_churn_rate,
      bundle_paying_churn_rate,
      bundle_platform_churn_rate,
      bundle_global_churn_rate
    ]
  }
}
