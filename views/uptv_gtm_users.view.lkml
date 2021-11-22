# The name of this view in Looker is "Gtm Users"
view: uptv_gtm_users {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: gilmore_the_merrier.gtm_users ;;
  drill_fields: [id]
  # This primary key is the unique key for this table in the underlying database.
  # You need to define a primary key in a view in order to join to other views.

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  # Here's what a typical dimension looks like in LookML.
  # A dimension is a groupable field that can be used to filter query results.
  # This dimension will be called "Access" in Explore.

  dimension: access {
    type: yesno
    sql: ${TABLE}.access ;;
  }

  dimension: activation {
    type: string
    sql: ${TABLE}.activation ;;
  }

  dimension: birthday {
    type: string
    sql: ${TABLE}.birthday ;;
  }

  # Dates and timestamps can be represented in Looker using a dimension group of type: time.
  # Looker converts dates and timestamps to the specified timeframes within the dimension group.

  dimension_group: date {
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
    sql: ${TABLE}.date ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
    tags: ["email"]
  }

  dimension: fname {
    type: string
    sql: ${TABLE}.fname ;;
  }


  dimension: ip {
    type: string
    sql: ${TABLE}.ip ;;
  }

  dimension_group: last_time_logged_in {
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
    sql: ${TABLE}.last_time_logged_in ;;
  }

  dimension: lname {
    type: string
    sql: ${TABLE}.lname ;;
  }


  dimension: receive_email {
    type: yesno
    sql: ${TABLE}.receive_email ;;
  }

  dimension: received_dm {
    type: string
    sql: ${TABLE}.received_dm ;;
  }

  dimension: twitter {
    type: string
    sql: ${TABLE}.twitter ;;
  }

  dimension: twitter_id {
    type: string
    sql: ${TABLE}.twitter_id ;;
  }

  measure: count {
    type: count
    drill_fields: [id, lname, fname]
  }
}
