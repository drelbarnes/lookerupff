view: bigquery_derived_signin {

  derived_table: {
    sql: (with a as
        (select a.timestamp,
                SAFE_CAST(a.user_id AS INT64) as user_id,
                b.platform,
                a.event,
                'Android' as source
         from android.signin as a left join customers.subscribers as b
         on SAFE_CAST(a.user_id AS INT64) = b.customer_id
         union all
         select a.timestamp,
                SAFE_CAST(a.user_id AS INT64) as user_id,
                b.platform,
                a.event,
                'iOS' as source
         from ios.signin as a left join customers.subscribers as b
         on SAFE_CAST(a.user_id AS INT64) = b.customer_id
         union all
         select a.timestamp,
                SAFE_CAST(a.user_id AS INT64) as user_id,
                b.platform,
                a.event,
                'Web' as source
         from javascript.authentication as a left join customers.subscribers as b
         on SAFE_CAST(a.user_id AS INT64) = b.customer_id
        )

select a.*, status
from a inner join customers.subscribers on SAFE_CAST(user_id AS INT64) = customer_id) ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }


  dimension_group: timestamp {
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
    sql: ${TABLE}.timestamp ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }

}
