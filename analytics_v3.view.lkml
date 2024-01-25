view: analytics_v3 {
  # Or, you could make this view a derived table, like this:
  derived_table: {
    sql: WITH date_parameters AS (
        SELECT
        {% date_start user_defined_date_range %}::DATE AS date_start,
        {% date_end user_defined_date_range %}::DATE AS date_end,
        {% if user_defined_compare_to_range._is_filtered %}
        {% date_start user_defined_compare_to_range %}::DATE AS comparison_date_start,
        {% date_end user_defined_compare_to_range %}::DATE AS comparison_date_end
        {% else %}
        NULL::DATE AS comparison_date_start,
        NULL::DATE AS comparison_date_end
        {% endif %}
      ),
      date_delta AS (
        SELECT
        date_start,
        date_end,
        comparison_date_start,
        comparison_date_end,
        {% if user_defined_compare_to_range._is_filtered %}
        (date_start - comparison_date_start) AS delta_days
        {% else %}
        NULL::INTEGER AS delta_days
        {% endif %}
        FROM date_parameters
      )
      , analytics_v2 as (
        select
        "timestamp"
        , total_free_trials
        , total_paying
        , free_trial_created
        , free_trial_converted
        , free_trial_churn
        , paying_created
        , paying_churn
        , paused_created
        , new_trials_14_days_prior
        , churn_30_days
        , paying_30_days_prior
        , day_of_year
        , week
        , month
        , year
        , quarter
        , rownum
        from ${analytics_v2.SQL_TABLE_NAME}
      )
      , daily_spend as (
      select
      date_start
      , spend
      from ${daily_spend.SQL_TABLE_NAME}
      group by 1,2
      )
      , ltv_cpa as (
        select
        "timestamp"
        , cpa
        , ltv
        from ${ltv_cpa.SQL_TABLE_NAME}
        group by 1,2,3
      )
      , kpis as (
        select
        a."timestamp"
        , a.total_free_trials
        , a.total_paying
        , a.free_trial_created
        , a.free_trial_converted
        , a.free_trial_churn
        , a.paying_created
        , a.paying_churn
        , a.paused_created
        , a.new_trials_14_days_prior
        , a.churn_30_days
        , a.paying_30_days_prior
        , a.day_of_year
        , a.week
        , a.month
        , a.year
        , a.quarter
        , b.spend
        , c.cpa
        , c.ltv
        from analytics_v2 a
        left join daily_spend b
        on a."timestamp" = date(b.date_start)
        left join ltv_cpa c
        on a."timestamp" = c."timestamp"
        order by a."timestamp" desc
      )
      , comparison_kpis as (
        SELECT
        k."timestamp"
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k."timestamp", dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_timestamp
        {% else %}
        , NULL::TIMESTAMP as comparison_timestamp
        {% endif %}
        , k.total_free_trials
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.total_free_trials, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_total_free_trials
        {% else %}
        , NULL::INTEGER as comparison_total_free_trials
        {% endif %}
        , k.total_paying
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.total_paying, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_total_paying
        {% else %}
        , NULL::INTEGER as comparison_total_paying
        {% endif %}
        , k.free_trial_created
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.free_trial_created, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_free_trial_created
        {% else %}
        , NULL::INTEGER as comparison_free_trial_created
        {% endif %}
        , k.free_trial_converted
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.free_trial_converted, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_free_trial_converted
        {% else %}
        , NULL::INTEGER as comparison_free_trial_converted
        {% endif %}
        , k.free_trial_churn
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.free_trial_churn, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_free_trial_churn
        {% else %}
        , NULL::INTEGER as comparison_free_trial_churn
        {% endif %}
        , k.paying_created
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.paying_created, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_paying_created
        {% else %}
        , NULL::INTEGER as comparison_paying_created
        {% endif %}
        , k.paying_churn
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.paying_churn, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_paying_churn
        {% else %}
        , NULL::INTEGER as comparison_paying_churn
        {% endif %}
        , k.paused_created
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.paused_created, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_paused_created
        {% else %}
        , NULL::INTEGER as comparison_paused_created
        {% endif %}
        , k.new_trials_14_days_prior
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.new_trials_14_days_prior, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_new_trials_14_days_prior
        {% else %}
        , NULL::INTEGER as comparison_new_trials_14_days_prior
        {% endif %}
        , k.churn_30_days
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.churn_30_days, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_churn_30_days
        {% else %}
        , NULL::INTEGER as comparison_churn_30_days
        {% endif %}
        , k.paying_30_days_prior
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.paying_30_days_prior, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_paying_30_days_prior
        {% else %}
        , NULL::INTEGER as comparison_paying_30_days_prior
        {% endif %}
        , k.day_of_year
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.day_of_year, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_day_of_year
        {% else %}
        , NULL::INTEGER as comparison_day_of_year
        {% endif %}
        , k.week
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.week, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_week
        {% else %}
        , NULL::INTEGER as comparison_week
        {% endif %}
        , k.month
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.month, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_month
        {% else %}
        , NULL::INTEGER as comparison_month
        {% endif %}
        , k.year
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.year, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_year
        {% else %}
        , NULL::INTEGER as comparison_year
        {% endif %}
        , k.quarter
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.quarter, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_quarter
        {% else %}
        , NULL::INTEGER as comparison_quarter
        {% endif %}
        , k.spend
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.spend, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_spend
        {% else %}
        , NULL::INTEGER as comparison_spend
        {% endif %}
        , k.cpa
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.cpa, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_cpa
        {% else %}
        , NULL::INTEGER as comparison_cpa
        {% endif %}
        , k.ltv
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.ltv, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_ltv
        {% else %}
        , NULL::INTEGER as comparison_ltv
        {% endif %}
        FROM
        kpis k, date_delta dd
      )
      select k.*
      from comparison_kpis k
      join date_delta dd
      on k."timestamp" between dd.date_start and dd.date_end
      ;;
  }

  # Filter for the primary date range
  filter: user_defined_date_range {
    type: date
    label: "Date Range"
  }

  # Filter for the comparison date range
  filter: user_defined_compare_to_range {
    type: date
    label: "Compare To (Date Range)"
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}."timestamp" ;;
  }

  dimension_group: comparison_timestamp {
    type: time
    sql: ${TABLE}.comparison_timestamp ;;
  }

  dimension: total_free_trials {
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: comparison_total_free_trials {
    type: number
    sql: ${TABLE}.comparison_total_free_trials ;;
  }

  dimension: total_paying {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  dimension: comparison_total_paying {
    type: number
    sql: ${TABLE}.comparison_total_paying ;;
  }

  dimension: free_trial_created {
    type: number
    sql: ${TABLE}.free_trial_created ;;
  }

  dimension: comparison_free_trial_created {
    type: number
    sql: ${TABLE}.comparison_free_trial_created ;;
  }

  dimension: free_trial_converted {
    type: number
    sql: ${TABLE}.free_trial_converted ;;
  }

  dimension: comparison_free_trial_converted {
    type: number
    sql: ${TABLE}.comparison_free_trial_converted ;;
  }

  dimension: free_trial_churn {
    type: number
    sql: ${TABLE}.free_trial_churn ;;
  }

  dimension: comparison_free_trial_churn {
    type: number
    sql: ${TABLE}.comparison_free_trial_churn ;;
  }

  dimension: paying_created {
    type: number
    sql: ${TABLE}.paying_created ;;
  }

  dimension: comparison_paying_created {
    type: number
    sql: ${TABLE}.comparison_paying_created ;;
  }

  dimension: paying_churn {
    type: number
    sql: ${TABLE}.paying_churn ;;
  }

  dimension: comparison_paying_churn {
    type: number
    sql: ${TABLE}.comparison_paying_churn ;;
  }

  dimension: paused_created {
    type: number
    sql: ${TABLE}.paused_created ;;
  }

  dimension: comparison_paused_created {
    type: number
    sql: ${TABLE}.comparison_paused_created ;;
  }

  dimension: new_trials_14_days_prior {
    type: number
    sql: ${TABLE}.new_trials_14_days_prior ;;
  }

  dimension: comparison_new_trials_14_days_prior {
    type: number
    sql: ${TABLE}.comparison_new_trials_14_days_prior ;;
  }

  dimension: churn_30_days {
    type: number
    sql: ${TABLE}.churn_30_days ;;
  }

  dimension: comparison_churn_30_days {
    type: number
    sql: ${TABLE}.comparison_churn_30_days ;;
  }

  dimension: paying_30_days_prior {
    type: number
    sql: ${TABLE}.paying_30_days_prior ;;
  }

  dimension: comparison_paying_30_days_prior {
    type: number
    sql: ${TABLE}.comparison_paying_30_days_prior ;;
  }

  dimension: day_of_year {
    type: number
    sql: ${TABLE}.day_of_year ;;
  }

  dimension: comparison_day_of_year {
    type: number
    sql: ${TABLE}.comparison_day_of_year ;;
  }

  dimension: week {
    type: number
    sql: ${TABLE}.week ;;
  }

  dimension: comparison_week {
    type: number
    sql: ${TABLE}.comparison_week ;;
  }

  dimension: month {
    type: number
    sql: ${TABLE}.month ;;
  }

  dimension: comparison_month {
    type: number
    sql: ${TABLE}.comparison_month ;;
  }

  dimension: year {
    type: number
    sql: ${TABLE}.year ;;
  }

  dimension: comparison_year {
    type: number
    sql: ${TABLE}.comparison_year ;;
  }

  dimension: quarter {
    type: number
    sql: ${TABLE}.quarter ;;
  }

  dimension: comparison_quarter {
    type: number
    sql: ${TABLE}.comparison_quarter ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: comparison_spend {
    type: number
    sql: ${TABLE}.comparison_spend ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: cpa {
    type: number
    sql: ${TABLE}.cpa ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: comparison_cpa {
    type: number
    sql: ${TABLE}.comparison_cpa ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: ltv {
    type: number
    sql: ${TABLE}.ltv ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: comparison_ltv {
    type: number
    sql: ${TABLE}.comparison_ltv ;;
    value_format: "$#.00;($#.00)"
  }
}
