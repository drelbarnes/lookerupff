view: analytics {
  derived_table: {
    sql:
    with ovation_subscriptions as(
      SELECT
        *
      FROM customers.ovationarts_all_customers
      ),
   vimeo_raw as (
      select
      CAST(email AS VARCHAR(255))
      ,platform
      ,CASE
      WHEN status = 'free_trial' THEN 'in_trial'
      WHEN status = 'expired' THEN 'cancelled'
      WHEN status = 'enabled' THEN 'active'
      ELSE status
      END AS status
      ,date(customer_created_at) AS created_at
      ,report_date
      from ovation_subscriptions
      where action = 'subscription' and action_type != 'free_access'
      ),

      result2 as (select
      email
      ,status
      ,platform
      ,created_at
      ,report_date
      ,CASE
      WHEN status = 'active' AND LAG(status) OVER (PARTITION BY email ORDER BY report_date) ='in_trial'
      THEN 'Yes'
      ELSE 'No'
      END AS trials_converted
      ,CASE
      WHEN status in('cancelled','paused') AND LAG(status) OVER (PARTITION BY email ORDER BY report_date) ='in_trial'
      THEN 'Yes'
      ELSE 'No'
      END AS trials_not_converted
      ,CASE
      WHEN status = 'active' AND LAG(status) OVER (PARTITION BY email ORDER BY report_date) in('cancelled','paused')
      THEN 'Yes'
      ELSE 'No'
      END AS re_acquisition
      ,CASE
      WHEN status in('cancelled','paused') AND LAG(status) OVER (PARTITION BY email ORDER BY report_date) ='active'
      THEN 'Yes'
      ELSE 'No'
      END AS sub_cancelled
      ,CASE
        WHEN created_at = report_date THEN 'Yes'
        ELSE 'No'
      END AS trial_created

      from vimeo_raw)

      select
      report_date
      ,email
      ,status
      ,platform
      ,created_at
      ,trials_converted
      ,trials_not_converted
      ,sub_cancelled
      ,re_acquisition
      ,trial_created
      from result2
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

  dimension: re_acquisitions_date {
    type: date
    sql: ${TABLE}.re_acquisition_date ;;
  }

  dimension: email {
    type: string
    sql:  ${TABLE}.email ;;
  }

  dimension: status {
    type:  string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }
  dimension: created_at {
    type: date
    sql: ${TABLE}.created_at ;;
  }

  dimension: trials_converted {
    type: string
    sql: ${TABLE}.trials_converted ;;
  }

  dimension: trial_created {
    type: string
    sql: ${TABLE}.trial_created ;;
  }

  dimension: trials_not_converted {
    type: string
    sql: ${TABLE}.trials_not_converted ;;
  }

  dimension: re_acquisitions {
    type: string
    sql:  ${TABLE}.re_acquisition ;;
  }

  dimension: user_cancelled {
    type: string
    sql:  ${TABLE}.sub_cancelled ;;
  }


  measure: total_paying {
    type: count_distinct
    # for Chargebee : active,non_rewing
    # for Vimeo : enabled
    filters: [status: "active,non_renewing,enabled"]
    sql:${TABLE}.email   ;;
  }

  measure: total_free_trials {
    type: count_distinct
    # for Chargebee : in_trial
    # for Vimeo : free_trial
    filters: [status: "in_trial,free_trial"]
    sql: ${TABLE}.email  ;;
  }

  measure: trials_converted_count {
    type: count_distinct
    filters: [trials_converted: "Yes"]
    sql: ${TABLE}.email  ;;
  }
  measure: trials_not_converted_count {
    type: count_distinct
    filters: [trials_not_converted: "Yes"]
    sql: ${TABLE}.email  ;;
  }

  measure: re_acquisitions_count {
    type: count_distinct
    filters: [re_acquisitions: "Yes"]
    sql: ${TABLE}.email  ;;
  }

  measure: user_cancelled_count {
    type: count_distinct
    filters: [user_cancelled: "Yes"]
    sql: ${TABLE}.email  ;;
  }

  measure: trial_created_count {
    type: count_distinct
    filters: [trial_created: "Yes"]
    sql: ${TABLE}.email  ;;
  }




}
