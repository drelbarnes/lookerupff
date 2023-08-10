view: set_cancellations {
    derived_table: {
      sql: with

              vimeo_purchase_event_p0 as
              (
              select
                user_id,
                topic,
                email,
                moptin,
                subscription_status,
                subscription_frequency,
                platform,
                row_number() over (partition by user_id order by timestamp asc) as event_num,
                date(timestamp) as date_stamp,
                subscription_frequency
              from http_api.purchase_event
              where user_id <> '0'
              and regexp_contains(user_id, r'^[0-9]*$')
              order by
                user_id,
                date(timestamp)
              ),

              vimeo_purchase_event_q0 as
              (
              select
                user_id,
                topic,
                email,
                moptin,
                subscription_status,
                subscription_frequency,
                platform,
                row_number() over (partition by user_id order by timestamp desc) as event_num,
                date(timestamp) as date_stamp
              from http_api.purchase_event
              where user_id <> '0'
              and regexp_contains(user_id, r'^[0-9]*$')
              order by
                user_id,
                date(timestamp)
              ),

              distinct_purchase_event as
              (
              select
                distinct user_id,
                topic,
                extract(month from date_stamp) as month,
                extract(year from date_stamp) as year
              from vimeo_purchase_event_p0
              ),

              audience_first_event as
              (
              select
                user_id,
                min(date_stamp) as first_event_date
              from vimeo_purchase_event_p0
              group by user_id
              ),

              audience_last_event as
              (
              select
                user_id,
                email,
                topic,
                moptin,
                platform,
                subscription_frequency,
                date_stamp as last_event
              from vimeo_purchase_event_q0
              where event_num = 1
              ),

              customers_updated_event as
              (
              select
                b.user_id,
                b.email,
                b.moptin,
                b.platform,
                b.topic as vimeo_status,
                b.subscription_frequency,
                b.last_event as last_event_date,
                c.first_event_date
              from audience_last_event as b
              left join audience_first_event as c
              on b.user_id = c.user_id
              )

              select
                last_event_date,
                count(distinct user_id) as number_set_cancel
              from customers_updated_event
              where vimeo_status = 'customer.product.set_cancellation'
              group by last_event_date
              order by last_event_date desc
              limit 14 ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: last_event_date {
      type: date
      datatype: date
      sql: ${TABLE}.last_event_date ;;
    }

    measure: number_set_cancel {
      type: sum
      sql: ${TABLE}.number_set_cancel ;;
    }

    set: detail {
      fields: [
        last_event_date,
        number_set_cancel
      ]
    }
  }
