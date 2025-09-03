view: sub_count_v2 {
  derived_table: {
    sql:
      SELECT
        sub_count_report_date_date as report_date
        ,sub_count_total_free_trials as total_free_trials
        ,sub_count_platform as platform
        ,sub_count_billing_period as billing_period
        ,sub_count_total_paying as total_paying
        from looker.upff_v2_paid_and_trials_aggregate
    ;;
  }
  dimension: date {
    type: date
    sql:  ${TABLE}.report_date ;;
  }
  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes  # Adjust for timezone conversion if needed
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }


  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }


  dimension: total_paying {
    type: number
    sql:${TABLE}.total_paying   ;;
  }

  dimension: total_free_trials {
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }
}
