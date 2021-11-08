# The name of this view in Looker is "Email Campaigns"
view: hubspot_email_campaigns {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  derived_table: {
    sql: SELECT *, (counters_open/counters_delivered) as open_rate FROM `up-faith-and-family-216419.hubspot.email_campaigns` ;;
  }
  drill_fields: [id]
  # This primary key is the unique key for this table in the underlying database.
  # You need to define a primary key in a view in order to join to other views.

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  # Dates and timestamps can be represented in Looker using a dimension group of type: time.
  # Looker converts dates and timestamps to the specified timeframes within the dimension group.

  dimension_group: _partitiondate {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}._PARTITIONDATE ;;
  }

  dimension_group: _partitiontime {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}._PARTITIONTIME ;;
  }

  # Here's what a typical dimension looks like in LookML.
  # A dimension is a groupable field that can be used to filter query results.
  # This dimension will be called "App ID" in Explore.

  dimension: app_id {
    type: number
    sql: ${TABLE}.app_id ;;
  }

  dimension: app_name {
    type: string
    sql: ${TABLE}.app_name ;;
  }

  dimension: content_id {
    type: number
    sql: ${TABLE}.content_id ;;
  }

  dimension: counters_bounce {
    type: number
    sql: ${TABLE}.counters_bounce ;;
  }

  # A measure is a field that uses a SQL aggregate function. Here are defined sum and average
  # measures for this dimension, but you can also add measures of many different aggregates.
  # Click on the type parameter to see all the options in the Quick Help panel on the right.

  measure: total_counters_bounce {
    type: sum
    sql: ${counters_bounce} ;;
  }

  measure: average_counters_bounce {
    type: average
    sql: ${counters_bounce} ;;
  }

  dimension: counters_click {
    type: number
    sql: ${TABLE}.counters_click ;;
  }

  dimension: counters_deferred {
    type: number
    sql: ${TABLE}.counters_deferred ;;
  }

  dimension: counters_delivered {
    type: number
    sql: ${TABLE}.counters_delivered ;;
  }

  dimension: counters_dropped {
    type: number
    sql: ${TABLE}.counters_dropped ;;
  }

  dimension: counters_mta_dropped {
    type: number
    sql: ${TABLE}.counters_mta_dropped ;;
  }

  dimension: counters_open {
    type: number
    sql: ${TABLE}.counters_open ;;
  }

  dimension: counters_processed {
    type: number
    sql: ${TABLE}.counters_processed ;;
  }

  dimension: counters_sent {
    type: number
    sql: ${TABLE}.counters_sent ;;
  }

  dimension: counters_spamreport {
    type: number
    sql: ${TABLE}.counters_spamreport ;;
  }

  dimension: counters_statuschange {
    type: number
    sql: ${TABLE}.counters_statuschange ;;
  }

  dimension: counters_unsubscribed {
    type: number
    sql: ${TABLE}.counters_unsubscribed ;;
  }

  dimension: group_id {
    type: number
    sql: ${TABLE}.group_id ;;
  }

  dimension_group: last_processing_finished {
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
    sql: ${TABLE}.last_processing_finished_at ;;
  }

  dimension_group: last_processing_started {
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
    sql: ${TABLE}.last_processing_started_at ;;
  }

  dimension_group: last_processing_state_change {
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
    sql: ${TABLE}.last_processing_state_change_at ;;
  }

  dimension_group: loaded {
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
    sql: ${TABLE}.loaded_at ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: num_included {
    type: number
    sql: ${TABLE}.num_included ;;
  }

  dimension: processing_state {
    type: string
    sql: ${TABLE}.processing_state ;;
  }

  dimension_group: received {
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

  dimension_group: scheduled {
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
    sql: ${TABLE}.scheduled_at ;;
  }

  dimension: subject {
    type: string
    sql: ${TABLE}.subject ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension_group: uuid_ts {
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
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: open_rate {
    type: number
    sql: ${TABLE}.open_rate ;;
  }

  measure: avg_open_rate {
    type: average
    value_format: "0\%"
    sql: (${TABLE}.open_rate * 100);;
  }

  measure: count {
    type: count
    drill_fields: [id, app_name, name, email_events.count]
  }
}
