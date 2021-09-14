view: recovery_rates_monthly {
  derived_table: {
    sql:

    /*
    * Results: Get users in July whose last two topics or status events are charge failed and renewed.
    * This query will assist in revealing the recovery rate from the Vimeo OTT dunning process.
    * Fetch the last two purchase or topic events for web users in the month of July.
    * Convert topic events to numeric and loop on row number timestamp
    */

    /* selects web customers in month of Jul21 */
   with a as (select ROW_NUMBER() OVER (ORDER BY timestamp DESC) row_num, user_id,
    CASE
        WHEN topic = 'customer.product.charge_failed' THEN 1
        WHEN topic = 'customer.product.expired' THEN 2
        WHEN topic = 'customer.product.free_trial_expired' THEN 3
        WHEN topic = 'customer.product.renewed' THEN 4
        WHEN topic = 'customer.product.free_trial_converted' THEN 5
        ELSE 6
      END AS status,
    status_date,
    subscription_status,
    seqnum,
    date(timestamp) as datestamp,
    CASE
      WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
      WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
    END as customer_type,
    max(seqnum) as seqnum_total
    from (select mc.*,
                 row_number() over (partition by user_id order by timestamp desc) seqnum
          from http_api.purchase_event as mc WHERE platform = 'web'
         ) mc
           WHERE seqnum IN (1,2)
          GROUP BY user_id, topic,status_date,created_at,subscription_status,seqnum,timestamp

    )

    select * from a

    ;;

    }

    dimension: user_id {
      primary_key: yes
      sql:  ${TABLE}.user_id ;;
    }

    dimension: status {
      type: number
      sql:  ${TABLE}.status ;;
    }


    dimension: charge_failed {
      type: number
      sql:  ${status} = 1 ;;
    }

    dimension: renewed {
      type: number
      sql: ${status} = 5 ;;
    }

    dimension: seqnum {
      type:  number
      sql: ${TABLE}.seqnum ;;
    }

    dimension: seqnum_total {
      type: number
      sql: ${TABLE}.seqnum_total ;;
    }

    dimension: customer_type {
      type: string
      sql: ${TABLE}.customer_type ;;
    }

    dimension_group: datestamp {
      type: time
      timeframes: [date, week, month]
      sql: ${TABLE}.datestamp ;;
    }

    measure: count_charge_failed_subs{
      type: count_distinct
      label: "Charge Failed Subs"
      sql:  CASE WHEN ${status} = 1
         THEN ${user_id}
       ELSE NULL
       END ;;
      filters: [recovery_rates_monthly.seqnum: "1"]
    }

    measure: count_charge_failed_free_trial_expired_subs{
      type: count_distinct
      label: "Charge Failed & Free Trial Expired Subs"
      sql:  CASE WHEN ${status} = 1 OR ${status} = 3
         THEN ${user_id}
       ELSE NULL
       END ;;
      filters: [recovery_rates_monthly.seqnum: "1"]
    }


    measure: count_charge_failed_expired_subs{
      type: count_distinct
      label: "Charge Failed & Expired Subs"
      sql:  CASE WHEN ${status} = 1 OR ${status} = 2
         THEN ${user_id}
       ELSE NULL
       END ;;
      filters: [recovery_rates_monthly.seqnum: "1", recovery_rates_monthly.customer_type: "Paid"]
    }

    measure: count_expired_subs{
      type: count_distinct
      sql:
        CASE WHEN ${seqnum} = 1 OR 2
         THEN ${user_id}
       ELSE NULL
       END ;;
      filters: [recovery_rates_monthly.status: "2", recovery_rates_monthly.seqnum_total: "2"]
    }


    measure: count_trial_converted_subs{
      type: count_distinct
      label:  "Free Trial Converted Subs"
      sql:
        CASE WHEN ${seqnum} = 1 OR ${seqnum} = 2
         THEN ${user_id}
       ELSE NULL
       END ;;
      filters: [recovery_rates_monthly.status: "5", recovery_rates_monthly.customer_type: "Trialist", recovery_rates_monthly.seqnum_total: "2"]
    }


    measure: count_renewed_subs{
      type: count_distinct
      label:  "Renewed Subs"
      sql:
        CASE WHEN ${seqnum} = 1 OR ${seqnum} = 2
         THEN ${user_id}
       ELSE NULL
       END ;;
      filters: [recovery_rates_monthly.status: "4", recovery_rates_monthly.customer_type: "Paid", recovery_rates_monthly.seqnum_total: "2"]
    }

  }
