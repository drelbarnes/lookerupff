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
explore: http_api_users {}
explore: identifies {}
explore: heartlandia {}
explore: viewership {}
explore: ads_compare {}
explore: lifetime_value {}
explore: churn_texts {}
explore: ltv_cpa {}
explore: customer_churn_percent {}
explore: android_play {}
explore: ios_play {}
explore: javascript_play {}
explore: all_play {}
explore: titles {}
explore: mvpd_subs {}
