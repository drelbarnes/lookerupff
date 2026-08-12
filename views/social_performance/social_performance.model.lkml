# Optional standalone model — UPTV normally merges include + explore into upff.model.lkml instead (docs/09 §3.2.1).
connection: "upff"

# PDT rebuild trigger for marketing_attribution_testv2 (see views/Marketing_Attribution_Test/marketing_attribution_testv2.view.lkml).
# UTC/GMT — matches Agorapulse ingest calendar days (not America/New_York).
datagroup: social_performance_daily  {
  sql_trigger: SELECT TO_CHAR(
                   CONVERT_TIMEZONE('UTC', 'UTC', GETDATE())
                   - INTERVAL '2 hour',
                   'YYYY-MM-DD'
               ) ;;
  max_cache_age: "24 hours"
}

include: "/views/social_performance/social_daily_snapshot.view.lkml"
include: "/views/social_performance/agorapulse_post_performance.view.lkml"
include: "/views/social_performance/social_performance.dashboard.lookml"
include: "/views/Marketing_Attribution_Test/marketing_attribution_testv2.view.lkml"

explore: social_daily_snapshot {
  label: "Social Daily Snapshot"
}

explore: agorapulse_post_performance {
  label: "Social Post Last 30"
}

explore: marketing_attribution_testv2 {
  label: "Marketing Attribution (Test v2)"
}
