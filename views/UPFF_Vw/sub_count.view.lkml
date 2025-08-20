view: sub_count {
  derived_table: {
    sql:
    WITH active as(
      SELECT
        report_date
        ,user_id
        ,platform
        ,billing_period
      FROM ${UPFF_analytics_Vw.SQL_TABLE_NAME}
      WHERE status in ( 'active','non_renewing','enabled')
        ),

      trial as (
      SELECT
      report_date
      ,user_id
      ,platform
      ,billing_period
      FROM ${free_trials.SQL_TABLE_NAME}
      ),

      active_ios as (
      SELECT * FROM ${ios.SQL_TABLE_NAME}
      ),

      active_count as (
      SELECT
      count(distinct user_id) as user_count
      ,report_date
      ,platform
      ,billing_period
      FROM active
      WHERE platform not in 'ios'
      GROUP BY 2,3,4

      UNION ALL

      SELECT
      paid_subscribers as user_count
      ,report_date
      ,'ios' as platform
      ,billing_period
      FROM active_ios

      ),

      trial_count as (
      SELECT
      count(distinct user_id) as user_count
      ,report_date
      ,platform
      ,billing_period
      FROM trial
      GROUP BY 2,3,4
      )

      SELECT
      *
      ,'active' as status
      FROM active_count

      UNION ALL

      SELECT
      *
      ,'in_trial' as status
      FROM trial_count
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

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
  }

  dimension: status {
    type:  string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }


  measure: total_paying {
    type: sum
    filters: [status: "active"]
    sql:${TABLE}.user_count   ;;
  }

  measure: total_free_trials {
    type: sum
    filters: [status: "in_trial"]
    sql: ${TABLE}.user_count ;;
  }

}
