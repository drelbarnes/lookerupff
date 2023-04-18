# The name of this view in Looker is "App Store Connect Aggregate"
view: app_store_connect_aggregate {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: `up-faith-and-family-216419.customers.app_store_connect_aggregate`
    ;;
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
  # This dimension will be called "App Store Connect Subscribers Ios" in Explore.

  dimension: app_store_connect_subscribers_ios {
    type: number
    sql: ${TABLE}.app_store_connect_subscribers_ios ;;
  }

  # A measure is a field that uses a SQL aggregate function. Here are defined sum and average
  # measures for this dimension, but you can also add measures of many different aggregates.
  # Click on the type parameter to see all the options in the Quick Help panel on the right.

  measure: total_app_store_connect_subscribers_ios {
    type: sum
    sql: ${app_store_connect_subscribers_ios} ;;
  }

  measure: average_app_store_connect_subscribers_ios {
    type: average
    sql: ${app_store_connect_subscribers_ios} ;;
  }

  dimension: app_store_connect_subscribers_total {
    type: number
    sql: ${TABLE}.app_store_connect_subscribers_total ;;
  }

  dimension: app_store_connect_subscribers_tvos {
    type: number
    sql: ${TABLE}.app_store_connect_subscribers_tvos ;;
  }

  # Dates and timestamps can be represented in Looker using a dimension group of type: time.
  # Looker converts dates and timestamps to the specified timeframes within the dimension group.

  dimension_group: report {
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
    sql: ${TABLE}.report_date ;;
  }

  dimension: vimeo_ott_subcribers_ios {
    type: number
    sql: ${TABLE}.vimeo_ott_subcribers_ios ;;
  }

  dimension: vimeo_ott_subcribers_tvos {
    type: number
    sql: ${TABLE}.vimeo_ott_subcribers_tvos ;;
  }

  dimension: vimeo_ott_subscribers_total {
    type: number
    sql: ${TABLE}.vimeo_ott_subscribers_total ;;
  }

  measure: count {
    type: count
    drill_fields: [id]
  }
}
