
# Optional standalone model — UPTV normally merges include + explore into upff.model.lkml instead (docs/09 §3.2.1).
connection: "upff"

# PDT rebuild trigger for marketing_attribution_test (see views/Marketing_Attribution_Test/marketing_attribution_test.view.lkml).
datagroup: social_performance_daily {
  sql_trigger: SELECT TO_CHAR(
                   CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE())
                   - INTERVAL '2 hour',
                   'YYYY-MM-DD'
               ) ;;
  max_cache_age: "24 hours"
}

include: "/views/social_performance/social_daily_snapshot.view.lkml"
include: "/views/social_performance/agorapulse_post_performance.view.lkml"
include: "/views/social_performance/free_trials_from_organic.view.lkml"
include: "/views/social_performance/social_performance.dashboard.lookml"
include: "/views/Marketing_Attribution_Test/marketing_attribution_test.view.lkml"

explore: social_daily_snapshot {
  label: "Social Daily Snapshot"
}

explore: agorapulse_post_performance {
  label: "Social Post Snapshot"
}

explore: free_trials_from_organic {
  label: "Free trials from organic (Segment)"
  view_name: free_trials_from_organic
  description: "Single-measure explore; the measure uses a templated date window (dashboard Date range → listen, or 30 days when unset in LookML). Add this include to upff in production, or set dashboard model: social_performance in this mirror."
}

explore: marketing_attribution_test {
  label: "Marketing Attribution (Social Performance)"
}

# If you use THIS file as the model, set dashboard model: to social_snapshot (not upff) in social_performance.dashboard.lookml.
