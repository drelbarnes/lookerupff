view: javascript_derived_timeupdate {
  derived_table: {
    explore_source: javascript_timeupdate {

      column: id {
        field: javascript_timeupdate.id
      }
      column: timestamp{
        field: javascript_timeupdate.timestamp_time
      }
      column: user_id {
        field: javascript_timeupdate.user_id
      }
      column: title {
        field: javascript_timeupdate.title
      }

      filters: {
        field: javascript_timeupdate.timestamp_time
        value: "1 days ago"
      }

    }
  #datagroup_trigger: javascript_timeupdate_datagroup
  #indexes: ["user_id", "timeupdate_date"]

  }

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
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

  dimension: user_id {
   type: number
   sql: ${TABLE}.user_id ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }


}
