connection: "upff"

# include all the views
include: "*.view"

# include all the dashboards
include: "*.dashboard"

datagroup: upff_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: upff_default_datagroup

explore: application_installed{
  join: signupstarted {
    type:  left_outer
    sql_on: ${application_installed.anonymous_id} = ${signupstarted.anonymous_id} ;;
    relationship: one_to_one
  }
}

explore: analytics{}
explore: analytics_v2 {}
explore: subscribed {}
explore: pages{}
explore: customers {}
explore: churn_reasons_aggregated {}
explore: churn_custom_reasons {}
explore: afinn_lexicon {}
explore: purchase_event {}
explore: users {}
explore: identifies {}
explore: play {}
explore: heartlandia {}
explore: viewership {}
