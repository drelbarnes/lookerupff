view: chargebee_vimeo_ott_id_mapping {
  derived_table: {
    sql: with

    /*
    a.customer_id, b.ott_user_id, b.product_id
        FROM customers.admin_tg_mw_6778_customers a
        left join customers.admin_tg_mw_6778_ott_users b
        on a.id = b.customer_id
        */

        cb_id as (
        select distinct customer_email, customer_id from `up-faith-and-family-216419.http_api.chargebee_subscriptions` where date(timestamp) = current_date -1 and subscription_subscription_items_0_item_price_id like '%UP%'
      ),

      vimeo as (
      select
        email
        ,cast(user_id as string) as user_id
      from `up-faith-and-family-216419.customers.all_customers_6_16_2025`
      where action = 'subscription'
      union all

      select
        email
        ,cast(user_id as string) as user_id
      from `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_created`


      ),

      vimeo_id as (
        select
          a.customer_id,
          b.user_id as ott_user_id
        FROM cb_id a
        LEFT JOIN (select distinct * from vimeo) b
        ON a.customer_email = b.email
      )
      SELECT
      customer_id
      ,ott_user_id
      , "27315" as product_id
      from vimeo_id

      ;;
    datagroup_trigger: upff_daily_refresh_datagroup
  }


  # Define your dimensions and measures here, like this:
  dimension: customer_id {
    description: "Chargebee Customer ID"
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: ott_user_id {
    description: "Vimeo OTT User ID"
    type: number
    sql: ${TABLE}.ott_user_id ;;
  }

  dimension: product_id {
    description: "OTT Product ID"
    type: number
    sql: ${TABLE}.product_id ;;
  }

}
