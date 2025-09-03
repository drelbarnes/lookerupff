view: free_trials_historical {
    derived_table: {
      sql:
          with trials as (SELECT * FROM ${free_trials.SQL_TABLE_NAME})

          SELECT
            user_id as email
            ,billing_period
            ,platform
            ,report_date + INTERVAL '7 day' as report_date
          FROM trials
;;
    }
    dimension: date {
      type: date
      primary_key: yes
      sql:  ${TABLE}.report_date ;;
    }
    dimension_group: report_date {
      type: time

      timeframes: [date, week]
      sql: ${TABLE}.report_date ;;
      convert_tz: yes  # Adjust for timezone conversion if needed
    }

    dimension: platform{
    type: string
    sql: ${TABLE}.platform ;;
    }
    dimension: billing_period{
      type: string
      sql: ${TABLE}.billing_period ;;
    }

    measure: trial_7_days_ago {
      type: count_distinct
      sql: ${TABLE}.user_id ;;
    }
  }
