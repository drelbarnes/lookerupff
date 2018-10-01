view: bigquery_derived_views {

  derived_table: {
    sql: (with a as
        (select a.timestamp,
                a.user_id,
                b.platform,
                'Android' as source
         from android.view as a left join customers.subscribers as b
         on SAFE_CAST(a.user_id AS INT64) = b.customer_id
        union all
        select a.timestamp,
                SAFE_CAST(a.user_id AS STRING),
                b.platform,
                'iOS' as source
         from ios.view as a left join customers.subscribers as b
         on user_id = b.customer_id
         union all
         select a.timestamp,
                a.user_id,
                b.platform,
                'Web' as source
         from javascript.pages as a left join customers.subscribers as b
         on SAFE_CAST(a.user_id AS INT64) = b.customer_id
        )

select a.*, status
from a inner join customers.subscribers on SAFE_CAST(user_id AS INT64) = customer_id) ;;
  }


  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }


  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: number
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
