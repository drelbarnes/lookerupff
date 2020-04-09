view: op_uplift_customers {
  derived_table: {
    sql: with a as
      (select user_id
      from javascript.order_completed
      where date(timestamp)>='2020-03-19'
      union all
      select user_id
      from android.order_completed
      where date(timestamp)>='2020-03-19'
      union all
      select safe_cast(user_id as string) as user_id
      from ios.order_completed
      where date(timestamp)>='2020-03-19'
      union all
      select user_id
      from roku.order_completed
      where date(timestamp)>='2020-03-19'),

      b as
      (select distinct user_id
      from a),

      c as
      (select distinct user_id
      from http_api.purchase_event
      where date(status_date)>='2020-03-19' and plan='none'),

      d as
      (select case when b.user_id is null then c.user_id else null end as user_id
      from c left join b on b.user_id=c.user_id)

      select distinct user_id from d
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

  set: detail {
    fields: [user_id]
  }
}
