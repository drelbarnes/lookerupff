view: web_trial_subscribers {
  derived_table: {
    explore_source: subscribed {

      column: subscriber_count {
        field: subscribed.count
      }

      column: subscriber_id {
        field: subscribed.id
      }

      derived_column: subscriber_total {
        sql: count(subscriber_id) ;;
      }
    }
  }
  dimension: subscriber_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscriber_total {
    type: number
    sql: ${TABLE}.subscriber_total ;;
  }
}
