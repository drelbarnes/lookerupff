view: analytics {
  derived_table: {
    sql:
    with ovation_subscriptions as(
      SELECT
        email,
        MAX(CAST(customer_created_at AS DATE)) AS customer_created_at,
        MAX(CAST(event_created_at AS DATE))    AS event_created_at,
        MAX(CAST(expiration_date AS DATE))    AS expiration_date
      FROM customers.ovationarts_all_customers
      WHERE action = 'subscription'and  action_type != 'free_access' and frequency != 'custom'
      GROUP BY email

      ),
    user_status as (
    SELECT
      email
      ,CASE
        WHEN DATEDIFF(day, customer_created_at, CURRENT_DATE) < 7 THEN 'in_trial'
        WHEN( expiration_date is NULL and event_created_at = customer_created_at)  or expiration_date > CURRENT_DATE  THEN 'enabled'
        ELSE 'cancelled'
      END AS status
      FROM ovation_subscriptions
      ),

    user_count as(
    SELECT
      count(distinct email) as user_count
      ,status
    FROM user_status
    GROUP BY status
    )
    SELECT
    *,
     CAST('AzZmVjUuQo25N2MFb' AS VARCHAR(64)) AS user_id
    from user_count
      ;;
  }



  dimension: user_id {
    type: string
    sql:  ${TABLE}.user_id ;;
  }

  dimension: status {
    type:  string
    sql: ${TABLE}.status ;;
  }

  dimension: user_count {
    type:  string
    sql: ${TABLE}.user_count ;;
  }



}
