view: social_daily_snapshot {
  label: "Social Daily Snapshot"

  sql_table_name: agorapulse_webhook.social_daily_snapshot ;;

  # Reporting day (UTC). Cast if your warehouse column is VARCHAR/TIMESTAMP.
  dimension_group: snapshot_date {
    label: "Snapshot date"
    type: time
    datatype: date
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.date ;;
  }

  dimension: brand {
    label: "Brand"
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: platform {
    label: "Platform"
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: profile_id {
    label: "Profile ID"
    type: string
    sql: ${TABLE}.profile_id ;;
  }

  dimension: profile_name {
    label: "Profile name"
    type: string
    sql: ${TABLE}.profile_name ;;
  }

  dimension: impressions {
    hidden: yes
    type: number
    sql: ${TABLE}.impressions ;;
  }

  measure: total_impressions {
    label: "Total impressions"
    type: sum
    sql: ${impressions} ;;
    value_format_name: decimal_0
    description: "Sum of impressions at profile-day grain (Agorapulse viewsCount). See docs/05 and docs/06."
  }
}
