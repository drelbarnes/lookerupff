view: roku_churn_segments {
  derived_table: {
    sql: with a as
      (select distinct user_id, email
      from http_api.purchase_event
      where topic in ('customer.product.expired','customer.product.cancelled','customer.product.disabled')),

      b as
      (select distinct user_id, email
      from http_api.purchase_event
      where plan='standard'),

      currentcustomers as
      (select b.user_id, b.email
      from b left join a on a.user_id=b.user_id
      where a.user_id is null),

      rokuusers as
      (select distinct a.user_id as contact_key, a.email, 1 as current_roku_user
      from currentcustomers as a inner join looker.roku_firstplays as b on a.user_id=b.user_id),

      a1 as
      (select distinct user_id, email
      from http_api.purchase_event
      where topic in ('customer.product.cancelled') and date(status_date)>=date_sub(current_date(),interval 7 day)),

      rokucancels as
      (select distinct a1.user_id as contact_key, a1.email, 1 as cancelled_roku_user
      from a1 inner join looker.roku_firstplays as b on a1.user_id=b.user_id)

      select case when a.contact_key is null then b.contact_key else a.contact_key end as contact_key,
            case when a.email is null then b.email else a.email end as email,
            case when current_roku_user is null then 0 else current_roku_user end as current_roku_user,
            case when cancelled_roku_user is null then 0 else cancelled_roku_user end as cancelled_roku_user
      from rokuusers as a full join rokucancels as b on a.contact_key=b.contact_key
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: contact_key {
    type: string
    sql: ${TABLE}.contact_key ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: current_roku_user {
    type: number
    sql: ${TABLE}.current_roku_user ;;
  }

  dimension: cancelled_roku_user {
    type: number
    sql: ${TABLE}.cancelled_roku_user ;;
  }

  set: detail {
    fields: [contact_key, email, current_roku_user, cancelled_roku_user]
  }
}
