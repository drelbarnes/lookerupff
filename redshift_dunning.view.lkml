view: redshift_dunning {
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
    CASE
      WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
      WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
    END as customer_type,
    max(seqnum) as seqnum_total
    from (select mc.*,
                 row_number() over (partition by user_id order by timestamp desc) seqnum
          from http_api.purchase_event  mc WHERE date(timestamp) between '2021-07-01' AND '2021-07-31' AND platform = 'web'
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

  measure: count_expired_subs{
    type: count_distinct
    sql:
        CASE WHEN ${seqnum} = 1 OR 2
         THEN ${user_id}
       ELSE NULL
       END ;;
    filters: [redshift_dunning.status: "2", redshift_dunning.seqnum_total: "2"]
  }

    measure: count_renewed_subs{
      type: count_distinct
      sql:
        CASE WHEN ${seqnum} = 1 OR 2
         THEN ${user_id}
       ELSE NULL
       END ;;
      filters: [redshift_dunning.status: "4", redshift_dunning.seqnum_total: "2"]
    }


}
