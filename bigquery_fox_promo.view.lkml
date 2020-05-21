view: bigquery_fox_promo {
  derived_table: {
    sql: with a as
      (select timestamp as date_visited,
             anonymous_id
      from javascript_upff_home.pages
      where context_page_path like '%fox%'),

      order_completed as
      (select timestamp as date_start,
             anonymous_id,
             user_id,
             'web' as source
      from javascript.order_completed
      union all
      select timestamp as date_start,
             anonymous_id,
             safe_cast(user_id as string) as user_id,
             'ios' as source
      from ios.order_completed
      union all
      select timestamp as date_start,
             anonymous_id,
             user_id,
             'android' as source
      from android.order_completed)

      select a.anonymous_id,
             user_id,
             date_visited,
             date_start,
             source
      from a left join order_completed as b on a.anonymous_id=b.anonymous_id
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: date_visited {
    type: time
    sql: ${TABLE}.date_visited ;;
  }

  dimension_group: date_start {
    type: time
    sql: ${TABLE}.date_start ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  measure: visits {
    type: count_distinct
    sql: ${anonymous_id} ;;
  }

  measure: trial_starts {
    type: count_distinct
    sql: ${user_id} ;;
  }

  set: detail {
    fields: [anonymous_id, user_id, date_visited_time, date_start_time, source]
  }
}
