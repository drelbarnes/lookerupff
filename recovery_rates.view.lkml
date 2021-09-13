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
              date(status_date) as failed_status
          FROM http_api.purchase_event
          GROUP BY 1,2,3,4,6,7
      ),

      aa AS (SELECT * FROM a WHERE topic IN ('customer.product.charge_failed', 'customer.product.expired') AND customer_type = 'Paid' and platform = 'web')

      SELECT
          pe.user_id,
          pe.topic,
          date(pe.status_date) as renewed_status,
          aa.failed_status
      FROM aa
      INNER JOIN http_api.purchase_event pe
      ON aa.user_id = pe.user_id
      WHERE pe.topic = 'customer.product.renewed'
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

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: renewed_status {
    type: date
    sql: ${TABLE}.renewed_status ;;
  }

  dimension: failed_status {
    type: date
    sql: ${TABLE}.failed_status ;;
  }

  set: detail {
    fields: [user_id, topic, renewed_status, failed_status]
  }
}
