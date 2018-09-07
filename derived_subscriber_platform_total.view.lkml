view: derived_subscriber_platform_total {
  derived_table: {
    sql: (with a as
        (select a.received_at,
                b.email,
                id,
                platform,
                'Android' as source
         from android.users as a left join customers.customers as b on a.id = b.customer_id
         union all
         select a.received_at,
                b.email,
                id,
                platform,
                'iOS' as source
         from ios.users as a left join customers.customers as b on a.id = b.customer_id
         union all
         select a.received_at,
                b.email,
                id,
                platform,
                'Web' as source
         from javascript.users as a left join customers.customers as b on a.id = b.customer_id)

select a.*, status
from a inner join customers.customers on id = customer_id) ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension_group: received_at {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.received_at ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }


  measure: number_of_platforms_by_user {
    type: count_distinct
    sql: ${platform};;
  }

# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      email,
      received_at_time,
      platform,
      source,
      id
    ]
  }
}
