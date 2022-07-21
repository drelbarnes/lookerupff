view: customer_record_analytics {
  derived_table: {
    sql: with analytics as (
        select
        date
        , platform
        , frequency
        , count(
          case
          when platform = "amazon_fire_tv" and status = "free_trial" and days_at_status = 1 and topic = "customer_product_free_trial_created" then topic
          when platform != "amazon_fire_tv" and days_at_status = 1 and topic = "customer_product_free_trial_created" then topic
          end
          ) as free_trial_created
        , count(case when days_at_status = 1 and topic = "customer_product_free_trial_expired" then topic end) as free_trial_churn
        , count(case when days_at_status = 1 and topic = "customer_product_free_trial_converted" then topic end) as free_trial_converted
        , count(case when days_at_status = 1 and topic = "customer_product_created" then topic end) as paying_created
        , count(case when days_at_status = 1 and topic in ("customer_product_expired", "customer_product_cancelled") then topic end) as paying_churn
        , count(case when days_at_status = 1 and topic = "customer_product_paused" then topic end) as paused_created
        , count(distinct case when status in ('enabled') and total_days_at_status <= 365 then user_id end) as total_paying
        , count(distinct case when status in ('free_trial') and total_days_at_status <= 14 then user_id end) as total_free_trials
        from ${customer_record.SQL_TABLE_NAME}
        group by 1,2,3
      )
      , metrics as (
        with summation as (
          select *
          , sum(free_trial_created) over (partition by platform, frequency order by date rows between 27 preceding and 14 preceding) as free_trial_created_14_day_sum_offset
          , sum(free_trial_converted) over (partition by platform, frequency order by date rows between 13 preceding and current row) as free_trial_converted_14_day_sum
          , lag(case when frequency ="monthly" then total_paying end, 30) over (partition by platform, frequency order by date) as total_monthly_paying_30_days_prior
          , lag(case when frequency ="yearly" then total_paying end, 365) over (partition by platform, frequency order by date) as total_yearly_paying_365_days_prior
          , lag(total_paying, 30) over (partition by platform, frequency order by date) as total_paying_30_days_prior
          , sum(case when frequency = "monthly" then paying_churn end) over (partition by platform, frequency order by date rows between 29 preceding and current row) as monthly_paying_churn_14_day_sum
          , sum(case when frequency = "yearly" then paying_churn end) over (partition by platform, frequency order by date rows between 364 preceding and current row) as yearly_paying_churn_14_day_sum
          , sum(paying_churn) over (partition by platform, frequency order by date rows between 29 preceding and current row) as paying_churn_14_day_sum
          from analytics
        )
        , churn_rates as (
          select *
          , ifnull(free_trial_converted_14_day_sum/nullif(free_trial_created_14_day_sum_offset,0),null) as free_trial_conversion_rate
          , ifnull(monthly_paying_churn_14_day_sum / nullif(total_monthly_paying_30_days_prior,0),null) as monthly_churn_rate
          , ifnull(yearly_paying_churn_14_day_sum / nullif(total_yearly_paying_365_days_prior,0),null) as yearly_churn_rate
          , ifnull(paying_churn_14_day_sum / nullif(total_paying_30_days_prior,0),null) as paying_churn_rate
          from summation
        )
        select *
        , avg(paying_churn_rate) over (partition by platform, date) as platform_churn_rate
        , avg(paying_churn_rate) over (partition by date) as global_churn_rate
        from churn_rates
      )
      , period as (
        select *
        from metrics
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

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: free_trial_created {
    type: number
    sql: ${TABLE}.free_trial_created ;;
  }

  dimension: free_trial_churn {
    type: number
    sql: ${TABLE}.free_trial_churn ;;
  }

  dimension: free_trial_converted {
    type: number
    sql: ${TABLE}.free_trial_converted ;;
  }

  dimension: paying_created {
    type: number
    sql: ${TABLE}.paying_created ;;
  }

  dimension: paying_churn {
    type: number
    sql: ${TABLE}.paying_churn ;;
  }

  dimension: paused_created {
    type: number
    sql: ${TABLE}.paused_created ;;
  }

  dimension: total_paying {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  dimension: total_free_trials {
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: free_trial_created_14_day_sum_offset {
    type: number
    sql: ${TABLE}.free_trial_created_14_day_sum_offset ;;
  }

  dimension: free_trial_converted_14_day_sum {
    type: number
    sql: ${TABLE}.free_trial_converted_14_day_sum ;;
  }

  dimension: total_monthly_paying_30_days_prior {
    type: number
    sql: ${TABLE}.total_monthly_paying_30_days_prior ;;
  }

  dimension: total_yearly_paying_365_days_prior {
    type: number
    sql: ${TABLE}.total_yearly_paying_365_days_prior ;;
  }

  dimension: total_paying_30_days_prior {
    type: number
    sql: ${TABLE}.total_paying_30_days_prior ;;
  }

  dimension: monthly_paying_churn_14_day_sum {
    type: number
    sql: ${TABLE}.monthly_paying_churn_14_day_sum ;;
  }

  dimension: yearly_paying_churn_14_day_sum {
    type: number
    sql: ${TABLE}.yearly_paying_churn_14_day_sum ;;
  }

  dimension: paying_churn_14_day_sum {
    type: number
    sql: ${TABLE}.paying_churn_14_day_sum ;;
  }

  dimension: free_trial_conversion_rate {
    type: number
    sql: ${TABLE}.free_trial_conversion_rate ;;
  }

  dimension: monthly_churn_rate {
    type: number
    sql: ${TABLE}.monthly_churn_rate ;;
  }

  dimension: yearly_churn_rate {
    type: number
    sql: ${TABLE}.yearly_churn_rate ;;
  }

  dimension: paying_churn_rate {
    type: number
    sql: ${TABLE}.paying_churn_rate ;;
  }

  dimension: platform_churn_rate {
    type: number
    sql: ${TABLE}.platform_churn_rate ;;
  }

  dimension: global_churn_rate {
    type: number
    sql: ${TABLE}.global_churn_rate ;;
  }

  set: detail {
    fields: [
      date_time,
      platform,
      frequency,
      free_trial_created,
      free_trial_churn,
      free_trial_converted,
      paying_created,
      paying_churn,
      paused_created,
      total_paying,
      total_free_trials,
      free_trial_created_14_day_sum_offset,
      free_trial_converted_14_day_sum,
      total_monthly_paying_30_days_prior,
      total_yearly_paying_365_days_prior,
      total_paying_30_days_prior,
      monthly_paying_churn_14_day_sum,
      yearly_paying_churn_14_day_sum,
      paying_churn_14_day_sum,
      free_trial_conversion_rate,
      monthly_churn_rate,
      yearly_churn_rate,
      paying_churn_rate,
      platform_churn_rate,
      global_churn_rate
    ]
  }
}
