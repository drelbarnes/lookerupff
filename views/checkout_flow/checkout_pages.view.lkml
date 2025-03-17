view: checkout_pages {
  derived_table: {
    sql:

      select
        context_page_path
        ,context_ip
        ,'checkout_page' as data_table
        ,timestamp
      from JavaScript_upentertainment_checkout.pages
      union all
      select
        context_page_path
        ,context_ip
        ,'order_completed' as data_table
        ,timestamp
      from JavaScript_upentertainment_checkout.order_completed
      UNION all
      SELECT
        url as context_page_path
        ,anonymous_id as context_ip
        ,'marketing' as data_table
        ,timestamp
      FROM javascript_upff_home.pages
      ;;
      }
  parameter: include_marketing_pages {
    type: string
    allowed_value: {
      label: "Yes"
      value: "yes"
    }
    allowed_value: {
      label: "No"
      value: "no"
    }
    default_value: "no"
  }

  dimension: date {
    type: date
    sql: ${TABLE}.timestamp ;;
  }

  dimension: context_page_path {
    type: string
    sql: ${TABLE}.context_page_path ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: data_table {
    type: string
    sql: ${TABLE}.data_table ;;
  }
  measure: marketing_page_count{
    type:  count_distinct
    sql:
    CASE
      WHEN {% parameter include_marketing_pages %} = 'yes' THEN
        CASE
          WHEN ${TABLE}.context_page_path LIKE '%upfaithandfamily.com%'
          THEN ${TABLE}.context_ip
          ELSE NULL
        END
      ELSE
        NULL
    END ;;
    label: "Marketing Site Count"

  }
  measure: plans_page_count {
    type: count_distinct
    sql:${TABLE}.context_ip;;
    filters:[context_page_path: "/,/index.php/welcome/plans"]
    label: "Plans Page Count"
  }


  measure: payment_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Payment Page Count"
    filters: [context_page_path: "/index.php/welcome/payment,/index.php/welcome/confirm_payment/upfaithandfamily/%"]
  }

  measure: create_account_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Create Account Page Count"
    filters: [context_page_path:
      "/index.php/welcome/create_account,
      /create_account/"]
}
  measure: select_payment_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Select Payment Page Count"
    filters: [context_page_path:
      "/index.php/welcome/select_payment,
      /payment"]
  }

  measure: upsell_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Up Sell Page Count"
    filters: [ context_page_path:
      "/index.php/welcome/up_sell,
      /index.php/welcome/up_sell/upfaithandfamily/monthly,
      /index.php/welcome/up_sell/upfaithandfamily/yearly,
      /up_sell",
      data_table: "order_completed"]
  }

  measure: confirmation_page_count {
    type: count_distinct
    sql: ${TABLE}.context_ip ;;
    label: "Confirmation Page Count"
    filters: [context_page_path: "/index.php/welcome/confirmation"]
  }



}
