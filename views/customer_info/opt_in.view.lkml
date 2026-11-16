view: opt_in {
  derived_table: {
    sql:

    WITH vimeo_optin AS (
    SELECT
        email,
        CASE
            WHEN marketing_opt_in THEN 'true'
            ELSE 'false'
        END AS marketing_opt_in,
        'false' AS sms_opt_in
    FROM vimeo_ott_webhook.customer_created
),

web_optin AS (
    SELECT
        oc.user_email AS email,
        COALESCE(LOWER(su.marketing_opt_in), 'false') AS marketing_opt_in,
        COALESCE(LOWER(su.sms_opt_in), 'false') AS sms_opt_in
    FROM javascript_upentertainment_checkout.order_completed oc
    LEFT JOIN javascript_upentertainment_checkout.signed_up su
        ON su.user_id = oc.user_id
)

SELECT * FROM vimeo_optin
UNION ALL
SELECT * FROM web_optin
;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: sms_opt_in {
    type: string
    sql: ${TABLE}.sms_opt_in ;;
  }

  dimension: marketing_opt_in {
    type: string
    sql: ${TABLE}.marketing_opt_in ;;
  }
}
