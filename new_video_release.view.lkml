view: new_video_release {
  derived_table: {
    sql: with a as
      (select user_id,
             max(status_date) as status_date
      from http_api.purchase_event
      group by 1),

      b as
      (select user_id,
             topic,
             email,
             max(status_date) as status_date,
             platform,
            subscription_frequency,
            name
      from http_api.purchase_event
      group by 1,2,3,5,6,7),

      c as
      (select distinct user_id,
                      date(created_at) as created_at
      from http_api.purchase_event)

      (select distinct b.user_id,
                       b.email,
                       created_at,
                       date(b.status_date) as most_recent_status_date,
                       topic,
                       platform,
                       subscription_frequency,
                       name,
                       case when date(b.status_date)=created_at then 'free trial' else 'paid sub' end as status,
      from a inner join b on a.user_id=b.user_id and a.status_date=b.status_date left join c on a.user_id=c.user_id
      where topic not in ('customer.product.disabled','customer.product.paused','customer.product.cancelled','customer.product.expired','customer.product.charge_failed','customer.created'))
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

  dimension: name{
    type: string
    sql: ${TABLE}.name;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: created_at {
    type: date
    sql: ${TABLE}.created_at ;;
  }

  dimension: most_recent_status_date {
    type: date
    sql: ${TABLE}.most_recent_status_date ;;
  }


  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: in_trial {
    case: {
      when: {
        sql: ${status} = 'free trial' ;;
        label: "Yes"
      }
      when: {
        sql: ${status} = 'paid sub' ;;
        label: "No"
      }

    }
  }



  set: detail {
    fields: [
      user_id,
      email,
      created_at,
      most_recent_status_date,
      topic,
      status
    ]
  }
}
