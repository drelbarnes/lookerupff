view: javascript_derived_timeupdate {
  derived_table: {
    explore_source: javascript_timeupdate {

      column: id {
        field: javascript_timeupdate.id
      }
      column: timestamp_date {
        field: javascript_timeupdate.timestamp_date
      }
      column: user_id {
        field: javascript_timeupdate.user_id
      }
      column: title {
        field: javascript_timeupdate.title
      }

      filters: {
        field: javascript_timeupdate.timestamp_date
        value: "1 days ago"
      }

    }
    #datagroup_trigger: javascript_timeupdate_datagroup
   # indexes: ["id", "timeupdate_time"]

  }

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: timestamp_date {
    type: date
    sql: ${TABLE}.timestamp_date ;;
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
