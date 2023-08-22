
view: bigquery_meta_offline_events {
  derived_table: {
    sql: with a as (
      SELECT email

      , properties_phone_value as phone
      , properties_firstname_value as first_name
      , properties_lastname_value as last_name

      FROM hubspot.contacts
      WHERE ((( received_at  ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -1 DAY))) AND ( received_at ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -1 DAY), INTERVAL 1 DAY)))))
      AND properties_phone_value IS NOT NULL

      ),

      b as (

      SELECT id as messageId

      , context_traits_email
      , context_revenue
      , transaction_id
      , order_id
      , context_ip

      FROM javascript.order_completed
      WHERE ((( received_at  ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -1 DAY))) AND ( received_at ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -1 DAY), INTERVAL 1 DAY)))))

      ),

      c as (

      select *
      from b LEFT JOIN a
      ON b.context_traits_email = a.email

      ),

      d as (

      SELECT email as pe_email
      , user_id
      , city
      , region
      , received_at as event_time

      FROM http_api.purchase_event
      WHERE ((( received_at  ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -1 DAY))) AND ( received_at ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -1 DAY), INTERVAL 1 DAY)))))
      AND topic = 'customer.product.free_trial_created'

      ),

      f as (

      select distinct

      row_number() over
        (
          partition by messageId
          order by event_time  desc
        )
        as load_messageId

      , email
      , (case WHEN phone LIKE '1%'
            THEN concat('+1', REPLACE(REPLACE(REPLACE(SUBSTRING(phone, 2), '(', ''), ')', ''), '-', ''))
            WHEN phone LIKE '+1%'
            THEN  REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', '')
            ELSE concat('+1', REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''))
       end) as phone
      , first_name as fn
      , last_name as ln
      , city as ct
      , region as st
      , 'US' as country
      , 'Purchase' as event_name
      , event_time
      , user_id as external_id
      , messageId as event_id
      , context_revenue as value
      , 'USD' as currency

      from d LEFT JOIN c
      ON d.pe_email = c.context_traits_email
      WHERE phone IS NOT NULL

      )

      SELECT email

      , phone
      , fn
      , ln
      , ct
      , st
      , country
      , event_name
      , event_time
      , external_id
      , event_id
      , currency
      , value

      FROM f
      WHERE load_messageId = 1 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: phone {
    type: string
    sql: ${TABLE}.phone ;;
  }

  dimension: fn {
    type: string
    sql: ${TABLE}.fn ;;
  }

  dimension: ln {
    type: string
    sql: ${TABLE}.ln ;;
  }

  dimension: ct {
    type: string
    sql: ${TABLE}.ct ;;
  }

  dimension: st {
    type: string
    sql: ${TABLE}.st ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  dimension: event_name {
    type: string
    sql: ${TABLE}.event_name ;;
  }

  dimension_group: event_time {
    type: time
    sql: ${TABLE}.event_time ;;
  }

  dimension: external_id {
    type: string
    sql: ${TABLE}.external_id ;;
  }

  dimension: event_id {
    type: string
    tags: ["user_id"]
    sql: ${TABLE}.event_id ;;
  }

  dimension: currency {
    type: string
    sql: ${TABLE}.currency ;;
  }

  dimension: value {
    type: number
    sql: ${TABLE}.value ;;
  }

  set: detail {
    fields: [
        email,
  phone,
  fn,
  ln,
  ct,
  st,
  country,
  event_name,
  event_time_time,
  external_id,
  event_id,
  currency,
  value
    ]
  }
}
