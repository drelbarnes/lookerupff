view: verizon_events {
  derived_table: {
    sql:
      WITH monthly_customer_report AS (
        select distinct user_id
        , email
        , first_name
        , last_name
        , city
        , state
        , country
        , product_id
        , product_name
        , action
        , action_type
        , status
        , frequency
        , platform
        , coupon_code
        , coupon_code_id
        , promotion_id
        , promotion_id_long
        , promotion_code
        , campaign
        , referrer
        , event_created_at
        , customer_created_at
        , expiration_date
        , marketing_opt_in
        , to_date(report_date, 'YYYY-MM-DD') as report_date
        from customers.all_customers
        where status = 'enabled'
        and report_date = current_date
      )
      , bango_events as (
        select
        id as event_id
        , event_date
        , bango_user_id
        , entitlement_id
        , user_id
        , product_key
        , reseller_key
        , bango_status
        , partner_id
        , partner
        , date_created
        , date_activated
        , date_ended
        , date_suspended
        , sent_at
        from looker.bango_events
        where date(sent_at) = current_date
      )
      , verizon_activate_pages as (
        select
        "timestamp"
        , path
        , anonymous_id
        , context_user_agent
        , context_ip
        , request_id
        from javascript_upff_home.pages
        where request_id is not null
        and path = '/partners/verizon'
      )
      , purchase_events as (
        select
        a.event_id
        , a.event_date
        , a.bango_user_id
        , a.entitlement_id
        , a.user_id
        , a.product_key
        , a.reseller_key
        , a.bango_status
        , a.partner_id
        , a.partner
        , a.date_created
        , a.date_activated
        , date_ended
        , date_suspended
        , b.email
        , b.first_name
        , b.last_name
        , b.city
        , b.state
        , b.country
        , c.anonymous_id
        , c.context_user_agent as user_agent
        , c.context_ip as ip_address
        , a.sent_at
        from bango_events a
        left join monthly_customer_report b on a.user_id = b.user_id
        left join verizon_activate_pages c on a.entitlement_id = c.request_id and date(a.event_date) = date(c.timestamp)
      )
      select * from purchase_events group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
    ;;
  }

  dimension: event_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.event_id ;;
  }

  dimension_group: event_date {
    type: time
    sql: ${TABLE}.event_date ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension: bango_user_id {
    type: string
    sql: ${TABLE}.bango_user_id ;;
  }

  dimension: entitlement_id {
    type: string
    sql: ${TABLE}.entitlement_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: product_key {
    type: string
    sql: ${TABLE}.product_key ;;
  }

  dimension: reseller_key {
    type: string
    sql: ${TABLE}.reseller_key ;;
  }

  dimension: bango_status {
    type: string
    sql: ${TABLE}.bango_status ;;
  }

  dimension: partner_id {
    type: string
    sql: ${TABLE}.partner_id ;;
  }

  dimension: partner {
    type: string
    sql: ${TABLE}.partner ;;
  }

  dimension_group: date_created {
    type: time
    sql: ${TABLE}.date_created ;;
  }

  dimension_group: date_activated {
    type: time
    sql: ${TABLE}.date_activated ;;
  }

  dimension_group: date_ended {
    type: time
    sql: ${TABLE}.date_ended ;;
  }

  dimension_group: date_suspended {
    type: time
    sql: ${TABLE}.date_suspended ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.ip_address ;;
  }

}
