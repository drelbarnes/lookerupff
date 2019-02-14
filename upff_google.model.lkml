connection: "google_bigquery_db"
include: "bigquery_subscribers_timeupdate.view.lkml"
include: "bigquery_derived_timeupdate.view.lkml"
include: "bigquery_derived_views.view.lkml"
include: "bigquery_derived_all_firstplay.view.lkml"
include: "bigquery_derived_addwatchlist.view.lkml"
include: "bigquery_delighted_survey_question_answered.view.lkml"
include: "bigquery_android_firstplay.view.lkml"
include: "bigquery_subscribers.view.lkml"
include: "bigquery_android_firstplay.view.lkml"
include: "predictions.view.lkml"
include: "bigquery_derived_signin.view.lkml"
include: "bigquery_views.view.lkml"
include: "bigquery_conversion_model_firstplay.view.lkml"
include: "bigquery_conversion_model_addwatchlist.view.lkml"
include: "bigquery_conversion_model_error.view.lkml"
include: "bigquery_conversion_model_removewatchlist.view.lkml"
include: "bigquery_conversion_model_timeupdate.view.lkml"
include: "bigquery_conversion_model_view.view.lkml"
include: "bigquery_subscribers_v2.view.lkml"
include: "bigquery_churn_model_customers.view.lkml"
include: "bigquery_churn_model_addwatchlist.view.lkml"
include: "bigquery_churn_model_error.view.lkml"
include: "bigquery_churn_model_firstplay.view.lkml"
include: "bigquery_churn_model_removewatchlist.view.lkml"
include: "bigquery_churn_model_timeupdate.view.lkml"
include: "bigquery_churn_model_view.view.lkml"
include: "bigquery_churn_model.view.lkml"
include: "bigquery_churn_model_predictions.view.lkml"
include: "model_performance.dashboard.lookml"
include: "churn_model_performance.dashboard.lookml"
include: "bigquery_marketing_cost.view"
include: "bigquery_allfirstplay.view.lkml"
include: "bigquery_timeupdate.view.lkml"
include: "bigquery_topmovies.view.lkml"
include: "bigquery_topseries.view.lkml"
include: "bigquery_prior_days_title_performance.view.lkml"
include: "bigquery_timeupdate_7day_vs_28day.view.lkml"
include: "bigquery_android_view.view.lkml"
include: "bigquery_android_users.view.lkml"
include: "bigquery_personas.view.lkml"
include: "bigquery_analytics.view.lkml"
include: "bigquery_firstplay.view.lkml"
include: "bigquery_ios_branch_install.view.lkml"
include: "bigquery_ios_branch_reinstall.view.lkml"
include: "bigquery_android_branch_install.view.lkml"
include: "bigquery_android_branch_reinstall.view.lkml"
include: "bigquery_clickthroughs.view.lkml"
include: "bigquery_conversions.view.lkml"
include: "bigquery_pixel_api_email_opened.view.lkml"
include: "bigquery_http_api_purchase_event.view.lkml"
include: "bigquery_quick_signup_subs.view.lkml"
include: "bigquery_php_get_user_on_email_list.view.lkml"
include: "bigquery_manual_subscribers_with_phone_numbers.view.lkml"
include: "bigquery_facebook_insights.view.lkml"
include: "bigquery_javascript_firstplay.view.lkml"
include: "bigquery_users.view.lkml"
include: "monthly_platform_user_count.view.lkml"
include: "bigquery_javascript_conversion.view.lkml"
include: "bigquery_javascript_pages.view.lkml"
include: "bigquery_javascript_users.view.lkml"
include: "bigquery_free_to_paid.view.lkml"
include: "bigquery_subscribers_v3.view.lkml"


# include: "bigquery_php_get_roku_firstplay.view.lkml"


include: "bigquery_http_api_get_roku_firstplay.view.lkml"

explore: monthly_platform_user_count {}

datagroup: upff_google_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
  sql_trigger: SELECT CURRENT_DATE() ;;
}
persist_with: upff_google_datagroup
explore: bigquery_clickthroughs {
  join: bigquery_conversions {
    type: inner
    sql_on: ${bigquery_clickthroughs.anonymous_id}=${bigquery_conversions.anonymous_id} ;;
    relationship: one_to_one
  }

}
explore: bigquery_android_branch_install {}
explore: bigquery_android_branch_reinstall {}
explore: bigquery_ios_branch_install {}
explore: bigquery_ios_branch_reinstall {}
explore: bigquery_firstplay {}
explore: bigquery_personas {}
explore: bigquery_derived_addwatchlist {}
explore: bigquery_derived_timeupdate {}
explore: bigquery_subscribers_timeupdate {}
explore: bigquery_derived_views {}

explore: bigquery_churn_model_error {
  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: cast(${bigquery_churn_model_error.user_id} as string)=${bigquery_http_api_purchase_event.user_id} ;;
    relationship: one_to_one
  }

}

explore: bigquery_timeupdate {
  join: bigquery_http_api_purchase_event {
    type: left_outer
    sql_on: ${bigquery_timeupdate.user_id}=${bigquery_http_api_purchase_event.user_id} ;;
    relationship: one_to_one
  }
}
explore: bigquery_topmovies {}
explore: bigquery_topseries {}
explore: bigquery_prior_days_title_performance {}
explore: bigquery_churn_model_view {}
explore: bigquery_timeupdate_7day_vs_28day {}

explore: bigquery_churn_model {}

explore: bigquery_subscribers_v2 {

  join: bigquery_conversion_model_addwatchlist {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.user_id} = ${bigquery_conversion_model_addwatchlist.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_error {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.user_id} = ${bigquery_conversion_model_error.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_firstplay {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.user_id} = ${bigquery_conversion_model_firstplay.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_removewatchlist {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.user_id} = ${bigquery_conversion_model_removewatchlist.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_timeupdate {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.user_id} = ${bigquery_conversion_model_timeupdate.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_view {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.user_id} = ${bigquery_conversion_model_view.user_id};;
    relationship: one_to_one
  }
}

  explore: bigquery_subscribers {
  label: "Subscribers"

  join: bigquery_android_view {
    relationship: many_to_one
    sql_on: SAFE_CAST(${bigquery_android_view.user_id} AS INT64) = ${bigquery_subscribers.customer_id} ;;
  }

  join: bigquery_android_users {
    type: inner
    relationship: one_to_one
    sql_on: SAFE_CAST(${bigquery_android_users.id} AS INT64) = ${bigquery_subscribers.customer_id} ;;
  }


  join: bigquery_derived_addwatchlist {
    type: inner
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_addwatchlist.user_id} AS INT64);;
    relationship: one_to_many
  }
  join: bigquery_derived_signin {
    type: left_outer
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_signin.user_id} AS INT64);;
    relationship: one_to_many
  }
  join: bigquery_derived_timeupdate {
    type: left_outer
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_timeupdate.user_id} AS INT64);;
    relationship: one_to_many
  }
  join: bigquery_subscribers_timeupdate {
    type: left_outer
    sql_on: ${bigquery_subscribers.customer_id} = ${bigquery_subscribers_timeupdate.user_id};;
    relationship: one_to_many
  }
  join: bigquery_derived_views {
    type: left_outer
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_views.user_id} AS INT64);;
    relationship: one_to_many
  }

}

explore: bigquery_allfirstplay {
  join: bigquery_http_api_purchase_event {
    type: left_outer
    sql_on: ${bigquery_allfirstplay.user_id}=${bigquery_http_api_purchase_event.user_id} ;;
    relationship: one_to_one
  }
  join: bigquery_analytics {
    type: left_outer
    sql_on: ${bigquery_allfirstplay.timestamp_date}=${bigquery_analytics.timestamp_date} ;;
    relationship: one_to_one
  }
}
explore: bigquery_derived_all_firstplay {
  join: bigquery_views{
    type: left_outer
    sql_on: ${bigquery_views.user_id} = SAFE_CAST(${bigquery_subscribers.customer_id} AS INT64);;
    relationship: one_to_one
  }
  join: bigquery_derived_views{
    type: left_outer
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_views.user_id} AS INT64);;
    relationship: one_to_many
  }
  join: bigquery_subscribers_timeupdate {
    type: left_outer
    sql_on: ${bigquery_subscribers_timeupdate.user_id} = SAFE_CAST(${bigquery_derived_all_firstplay.user_id} AS INT64);;
    relationship: one_to_one
  }
  join: bigquery_derived_timeupdate{
    type: left_outer
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_timeupdate.user_id} AS INT64);;
    relationship: one_to_many
  }
  join: bigquery_derived_addwatchlist {
    type: left_outer
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_addwatchlist.user_id} AS INT64);;
    relationship: one_to_many
  }
  join: bigquery_derived_signin {
    type: left_outer
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_signin.user_id} AS INT64);;
    relationship: one_to_many
  }
  join: bigquery_subscribers {
    type:  left_outer
    sql_on:  SAFE_CAST(${bigquery_subscribers.customer_id} AS INT64) = SAFE_CAST(${bigquery_derived_all_firstplay.user_id} AS INT64);;
    relationship: one_to_one
  }
  join: bigquery_delighted_survey_question_answered {
    type: left_outer
    sql_on: ${bigquery_delighted_survey_question_answered.user_id} = ${bigquery_derived_all_firstplay.user_id};;
    relationship: one_to_one
  }
  join: future_purchase_prediction {
    relationship: one_to_one
    sql_on: ${bigquery_derived_all_firstplay.user_id} = ${future_purchase_prediction.user_id} ;;
  }
}
explore: bigquery_android_firstplay {
  label: "First Play"
}

explore: bigquery_conversion_model_firstplay {

  join: bigquery_conversion_model_addwatchlist {
    type: left_outer
    sql_on: ${bigquery_conversion_model_firstplay.user_id} = ${bigquery_conversion_model_addwatchlist.user_id};;
    relationship: many_to_one
  }

  join: bigquery_conversion_model_error {
    type: left_outer
    sql_on: ${bigquery_conversion_model_firstplay.user_id} = ${bigquery_conversion_model_error.user_id};;
    relationship: many_to_one
  }

  join: bigquery_conversion_model_removewatchlist {
    type: left_outer
    sql_on: ${bigquery_conversion_model_firstplay.user_id} = ${bigquery_conversion_model_removewatchlist.user_id};;
    relationship: many_to_one
  }

  join: bigquery_conversion_model_timeupdate {
    type: left_outer
    sql_on: ${bigquery_conversion_model_firstplay.user_id} = ${bigquery_conversion_model_timeupdate.user_id};;
    relationship: many_to_one
  }

  join: bigquery_conversion_model_view {
    type: left_outer
    sql_on: ${bigquery_conversion_model_firstplay.user_id} = ${bigquery_conversion_model_view.user_id};;
    relationship: many_to_one
  }

}

explore: bigquery_marketing_cost {}

explore: bigquery_pixel_api_email_opened {


  label: "Email Opens > Conversions"

  join: bigquery_clickthroughs {
    type: inner
    sql_on: ${bigquery_clickthroughs.anonymous_id} = ${bigquery_conversions.anonymous_id};;
    relationship: many_to_many
  }


  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: ${bigquery_pixel_api_email_opened.user_id} = ${bigquery_http_api_purchase_event.user_id} AND (DATE_DIFF(${bigquery_http_api_purchase_event.received_date}, ${bigquery_pixel_api_email_opened.received_date}, DAY) <= 3);;
    #Attribution window of 15 Days
    relationship: many_to_many
  }


  join: bigquery_conversions {
    type: inner
    sql_on: ${bigquery_http_api_purchase_event.user_id} != ${bigquery_conversions.user_id};;
    relationship: many_to_many
  }

}

explore: bigquery_quick_signup_subs{

  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: ${bigquery_quick_signup_subs.user_id} = ${bigquery_http_api_purchase_event.user_id};;
    #Attribution window of 15 Days
    relationship: one_to_many
  }

}

explore: bigquery_php_get_user_on_email_list {

  label: "Quick Sign-ups Subscribers"

  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: ${bigquery_php_get_user_on_email_list.email} = ${bigquery_http_api_purchase_event.email};;
    relationship: one_to_many
  }

}

explore: bigquery_manual_subscribers_with_phone_numbers {

  label: "Subscribers with Phone Numbers"

  join: bigquery_subscribers {
    type: inner
    sql_on: ${bigquery_manual_subscribers_with_phone_numbers.email} = ${bigquery_subscribers.email};;
    relationship: one_to_one
  }

  join: bigquery_derived_signin {
    type: inner
    sql_on: ${bigquery_subscribers.customer_id} = ${bigquery_derived_signin.user_id};;
    relationship: one_to_one
  }

  join: bigquery_derived_addwatchlist {
    type: inner
    sql_on: ${bigquery_subscribers.customer_id} = ${bigquery_derived_addwatchlist.user_id};;
    relationship: one_to_one
  }

  join: bigquery_derived_timeupdate {
    type: inner
    sql_on: ${bigquery_subscribers.customer_id} = ${bigquery_derived_timeupdate.user_id};;
    relationship: one_to_one
  }

  join: bigquery_derived_views {
    type: inner
    sql_on: ${bigquery_subscribers.customer_id} = ${bigquery_derived_views.user_id};;
    relationship: one_to_one
  }

  join: bigquery_subscribers_timeupdate {
    type: inner
    sql_on: ${bigquery_subscribers.customer_id} = ${bigquery_subscribers_timeupdate.user_id};;
    relationship: one_to_one
  }

}

explore: bigquery_facebook_insights {}
explore: bigquery_javascript_firstplay {

  join: bigquery_users {
    type: inner
    sql_on: ${bigquery_javascript_firstplay.user_id} = ${bigquery_users.id};;
    relationship: one_to_many
  }

}

explore: bigquery_javascript_conversion {}
explore: bigquery_javascript_pages {

  join: bigquery_javascript_conversion {
    type: inner
    sql_on: ${bigquery_javascript_conversion.anonymous_id} = ${bigquery_javascript_pages.anonymous_id};;
    relationship: one_to_one
  }

  join: bigquery_javascript_users {
    type: inner
    sql_on: ${bigquery_javascript_users.context_traits_cross_domain_id} = ${bigquery_javascript_conversion.context_traits_cross_domain_id};;
    relationship: one_to_one
  }

  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: ${bigquery_http_api_purchase_event.email} = ${bigquery_javascript_users.email};;
    relationship: one_to_one
  }

}

explore: bigquery_free_to_paid {}
explore: bigquery_http_api_get_roku_firstplay {}

# include all views in this project
# include: "my_dashboard.dashboard.lookml"   # include a LookML dashboard called my_dashboard
# # Select the views that should be a part of this model,
# # and define the joins that connect them together.
#
# explore: order_items {
#   join: orders {
#     relationship: many_to_one
#     sql_on: ${orders.id} = ${order_items.order_id} ;;
#   }
#
#   join: users {
#     relationship: many_to_one
#     sql_on: ${users.id} = ${orders.user_id} ;;
#   }
# }
