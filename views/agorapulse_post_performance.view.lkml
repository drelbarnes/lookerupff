view: agorapulse_post_performance {
  label: "Social Post Performance"

  sql_table_name: agorapulse_webhook.agorapulse_post_performance ;;

  # Post publish time from Agorapulse content row (Segment properties). Cast if VARCHAR.
  dimension_group: publishing {
    label: "Publish date"
    type: time
    datatype: timestamp
    timeframes: [raw, date, week, month, quarter, year]
    sql: (${TABLE}.publishingDate)::timestamp ;;
  }

  dimension: brand {
    hidden: yes
    label: "Brand (warehouse raw)"
    type: string
    sql: ${TABLE}.brand ;;
    description: "Value as stored in Redshift. Use brand_canonical for filters aligned with Social Daily Snapshot."
  }

  dimension: brand_canonical {
    label: "Brand"
    type: string
    sql:
      CASE
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('ovation', 'ovation tv', 'ovationtv') THEN 'Ovation TV'
        WHEN LOWER(TRIM(${TABLE}.brand)) IN ('aspire', 'aspire tv', 'aspiretv') THEN 'Aspire TV'
        ELSE ${TABLE}.brand
      END ;;
    description: "Same normalization as social_daily_snapshot.brand_canonical so dashboard Brand filter matches both explores."
  }

  dimension: platform {
    label: "Platform"
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: post_id {
    label: "Post ID"
    type: string
    sql: ${TABLE}.post_id ;;
  }

  dimension: job_type {
    label: "Job type"
    type: string
    sql: ${TABLE}.job_type ;;
    description: "Ingest job: e.g. backfill, last_7 (future)."
  }

  measure: total_posts {
    label: "Total posts"
    type: number
    sql: COUNT(DISTINCT CASE WHEN ${TABLE}.post_id IS NOT NULL AND ${TABLE}.post_id <> '' THEN ${TABLE}.post_id END) ;;
    value_format_name: decimal_0
    description: "Distinct posts for current filters. Multiple Segment rows per post (backfill + rolling windows) count once."
  }
}
