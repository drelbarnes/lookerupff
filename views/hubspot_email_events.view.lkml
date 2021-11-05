# The name of this view in Looker is "Email Events"
view: hubspot_email_events {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: `up-faith-and-family-216419.hubspot.email_events`
    ;;
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

  dimension: attempt {
    type: number
    sql: ${TABLE}.attempt ;;
  }

  dimension: browser_family {
    type: string
    sql: ${TABLE}.browser_family ;;
  }

  dimension: browser_name {
    type: string
    sql: ${TABLE}.browser_name ;;
  }

  dimension: browser_producer {
    type: string
    sql: ${TABLE}.browser_producer ;;
  }

  dimension: browser_producer_url {
    type: string
    sql: ${TABLE}.browser_producer_url ;;
  }

  dimension: browser_type {
    type: string
    sql: ${TABLE}.browser_type ;;
  }

  dimension: browser_url {
    type: string
    sql: ${TABLE}.browser_url ;;
  }

  dimension: browser_version_0 {
    type: string
    sql: ${TABLE}.browser_version_0 ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension_group: caused_by_created {
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
    sql: ${TABLE}.caused_by_created ;;
  }

  dimension: caused_by_id {
    type: string
    sql: ${TABLE}.caused_by_id ;;
  }

  dimension_group: created {
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
    sql: ${TABLE}.created ;;
  }

  dimension: device_type {
    type: string
    sql: ${TABLE}.device_type ;;
  }

  dimension: drop_message {
    type: string
    sql: ${TABLE}.drop_message ;;
  }

  dimension: drop_reason {
    type: string
    sql: ${TABLE}.drop_reason ;;
  }

  dimension: duration {
    type: number
    sql: ${TABLE}.duration ;;
  }

  # A measure is a field that uses a SQL aggregate function. Here are defined sum and average
  # measures for this dimension, but you can also add measures of many different aggregates.
  # Click on the type parameter to see all the options in the Quick Help panel on the right.

  measure: total_duration {
    type: sum
    sql: ${duration} ;;
  }

  measure: average_duration {
    type: average
    sql: ${duration} ;;
  }

  dimension: email_campaign_group_id {
    type: number
    sql: ${TABLE}.email_campaign_group_id ;;
  }

  dimension: email_campaign_id {
    type: number
    # hidden: yes
    sql: ${TABLE}.email_campaign_id ;;
  }

  dimension: filtered_event {
    type: yesno
    sql: ${TABLE}.filtered_event ;;
  }

  dimension: from {
    type: string
    sql: ${TABLE}.`from` ;;
  }

  dimension: link_id {
    type: number
    sql: ${TABLE}.link_id ;;
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

  dimension: location_city {
    type: string
    sql: ${TABLE}.location_city ;;
  }

  dimension: location_country {
    type: string
    sql: ${TABLE}.location_country ;;
  }

  dimension: location_latitude {
    type: number
    sql: ${TABLE}.location_latitude ;;
  }

  dimension: location_longitude {
    type: number
    sql: ${TABLE}.location_longitude ;;
  }

  dimension: location_state {
    type: string
    sql: ${TABLE}.location_state ;;
  }

  dimension: location_zipcode {
    type: string
    sql: ${TABLE}.location_zipcode ;;
  }

  dimension_group: obsoleted_by_created {
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
    sql: ${TABLE}.obsoleted_by_created ;;
  }

  dimension: obsoleted_by_id {
    type: string
    sql: ${TABLE}.obsoleted_by_id ;;
  }

  dimension: portal_id {
    type: number
    sql: ${TABLE}.portal_id ;;
  }

  dimension: portal_subscription_status {
    type: string
    sql: ${TABLE}.portal_subscription_status ;;
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

  dimension: recipient {
    type: string
    sql: ${TABLE}.recipient ;;
  }

  dimension: referer {
    type: string
    sql: ${TABLE}.referer ;;
  }

  dimension: reply_to_0 {
    type: string
    sql: ${TABLE}.reply_to_0 ;;
  }

  dimension: response {
    type: string
    sql: ${TABLE}.response ;;
  }

  dimension_group: sent_by_created {
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
    sql: ${TABLE}.sent_by_created ;;
  }

  dimension: sent_by_id {
    type: string
    sql: ${TABLE}.sent_by_id ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: source_id {
    type: string
    sql: ${TABLE}.source_id ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: subject {
    type: string
    sql: ${TABLE}.subject ;;
  }

  dimension: subscriptions_0_id {
    type: number
    sql: ${TABLE}.subscriptions_0_id ;;
  }

  dimension: subscriptions_0_legal_basis_change_legal_basis_explanation {
    type: string
    sql: ${TABLE}.subscriptions_0_legal_basis_change_legal_basis_explanation ;;
  }

  dimension: subscriptions_0_legal_basis_change_legal_basis_type {
    type: string
    sql: ${TABLE}.subscriptions_0_legal_basis_change_legal_basis_type ;;
  }

  dimension: subscriptions_0_legal_basis_change_opt_state {
    type: string
    sql: ${TABLE}.subscriptions_0_legal_basis_change_opt_state ;;
  }

  dimension: subscriptions_0_status {
    type: string
    sql: ${TABLE}.subscriptions_0_status ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
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

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: user_count {
    type: count_distinct
    sql: ${recipient} ;;
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      browser_name,
      app_name,
      email_campaigns.id,
      email_campaigns.app_name,
      email_campaigns.name
    ]
  }
}
