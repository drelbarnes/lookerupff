view: recovery_rates {
  derived_table: {
    sql: WITH /*** shows recover rates of customers who were in topic charged failed one week prior to being renewed  ***/

      a AS
      (
        SELECT
              user_id, /*1*/
              email, /*2*/
          CASE WHEN moptin THEN 'Yes' ELSE 'No' END AS marketing_optin, /*3*/
              platform, /*4*/
              max(topic) AS topic, /*5*/
          CASE
              WHEN datediff('day', created_at, status_date) <= 14 THEN 'Trialist'
              WHEN datediff('day', created_at, status_date) > 14 THEN 'Paid'
              END as customer_type, /*6*/
              date(status_date) as failed_status, /*7*/
          CASE
              WHEN topic = 'customer.product.charge_failed' THEN 1
              WHEN topic = 'customer.product.expired' THEN 2
              WHEN topic = 'customer.product.free_trial_expired' THEN 3
              WHEN topic = 'customer.product.renewed' THEN 4
              WHEN topic = 'customer.product.free_trial_converted' THEN 5
              ELSE 6
              END AS status /*8*/
        FROM http_api.purchase_event
        GROUP BY 1,2,3,4,6,7,8
      ),

      aa AS (SELECT * FROM a WHERE topic IN ('customer.product.charge_failed', 'customer.product.expired') AND customer_type = 'Paid' and platform = 'web'),

      pe AS
      (
        SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) AS seqnum
        FROM http_api.purchase_event
      )


      SELECT
          pe.user_id,
          pe.topic,
          pe.seqnum,
          max(pe.seqnum) AS seqnum_total,
          date(pe.status_date) AS renewed_status,
          aa.failed_status,
          aa.customer_type,
          aa.status
      FROM aa
      INNER JOIN pe
      ON aa.user_id = pe.user_id
      WHERE pe.seqnum in (1,2)
      GROUP BY pe.user_id, pe.topic, aa.status, aa.failed_status, aa.customer_type, pe.seqnum, pe.status_date
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: status {
    type: number
    sql:  ${TABLE}.status ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: seqnum {
    type:  number
    sql: ${TABLE}.seqnum ;;
  }

  dimension: seqnum_total {
    type: number
    sql: ${TABLE}.seqnum_total ;;
  }

  dimension: renewed_status {
    type: date
    sql: ${TABLE}.renewed_status ;;
  }

  dimension: failed_status {
    type: date
    sql: ${TABLE}.failed_status ;;
  }

  dimension: customer_type {
    type: string
    sql: ${TABLE}.customer_type ;;
  }

  measure: count_charge_failed_subs {
    type: count_distinct
    label: "Charge Failed Subs"
    sql:  CASE WHEN ${status} = 1
         THEN ${user_id}
       ELSE NULL
       END ;;
  }

  measure: count_renewed_subs{
    type: count_distinct
    label:  "Renewed Subs"
    sql:
        CASE WHEN ${seqnum} = 1 OR ${seqnum} = 2
         THEN ${user_id}
       ELSE NULL
       END ;;
    filters: [recovery_rates.status: "4", recovery_rates.customer_type: "Paid", recovery_rates.seqnum_total: "2"]
  }

  set: detail {
    fields: [user_id, topic, renewed_status, failed_status]
  }
}
