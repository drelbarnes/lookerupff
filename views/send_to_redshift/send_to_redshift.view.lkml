view: send_to_redshift {
  derived_table: {
    sql:
    SELECT user_email,
    card_postal_code,
    'AzZmVjUuQo25N2MFb' as user_id
    FROM `up-faith-and-family-216419.customers.upff_vimeo_ott_payments_365days_2_24_2026` ;;
  }

  dimension: user_email {
    type: string
    sql: ${TABLE}.user_email ;;
  }

  dimension: card_postal_code {
    type: number
    sql: ${TABLE}.card_postal_code ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    tags: ["user_id"]
  }
  }
