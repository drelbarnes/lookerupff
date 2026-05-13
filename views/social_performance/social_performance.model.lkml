# Optional standalone model — UPTV normally merges include + explore into upff.model.lkml instead (docs/09 §3.2.1).
connection: "upff"

include: "/views/social_performance/social_daily_snapshot.view.lkml"
include: "/views/social_performance/agorapulse_post_performance.view.lkml"
include: "/views/social_performance/free_trials_from_organic.view.lkml"
include: "/views/social_performance/social_performance.dashboard.lookml"

explore: social_daily_snapshot {
  label: "Social Daily Snapshot"
}

explore: agorapulse_post_performance {
  label: "Social Post Snapshot"
}

explore: free_trials_from_organic {
  label: "Free trials from organic (Segment)"
  view_name: free_trials_from_organic
  description: "Single-measure explore; the measure uses a templated date window (dashboard Date range → listen, or 30 days when unset in LookML). Add this include to upff in production, or set dashboard model: social_snapshot in this mirror."
}

# If you use THIS file as the model, set dashboard model: to social_snapshot (not upff) in social_performance.dashboard.lookml.
