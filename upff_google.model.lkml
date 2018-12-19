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
explore: bigquery_allfirstplay {
  join: bigquery_analytics {
    type: left_outer
    sql_on: ${bigquery_allfirstplay.timestamp_date}=${bigquery_analytics.timestamp_date} ;;
    relationship: one_to_one
  }
}
explore: bigquery_timeupdate {}
explore: bigquery_topmovies {}
explore: bigquery_topseries {}
explore: bigquery_prior_days_title_performance {}
explore: bigquery_churn_model_view {}
explore: bigquery_timeupdate_7day_vs_28day {}

explore: bigquery_churn_model {}

explore: bigquery_churn_model_customers {

  join: bigquery_churn_model_addwatchlist {
    type: left_outer
    sql_on: ${bigquery_churn_model_customers.customer_id}=safe_cast(${bigquery_churn_model_addwatchlist.user_id} as int64) and
    ${bigquery_churn_model_addwatchlist.timestamp_date} between ${bigquery_churn_model_customers.start_date} and ${bigquery_churn_model_customers.end_date};;
    relationship: one_to_one
  }

  join: bigquery_churn_model_error {
    type: left_outer
    sql_on: ${bigquery_churn_model_customers.customer_id}=safe_cast(${bigquery_churn_model_error.user_id} as int64) and
            ${bigquery_churn_model_error.timestamp_date} between ${bigquery_churn_model_customers.start_date} and ${bigquery_churn_model_customers.end_date};;
    relationship: one_to_one
  }
}


explore: bigquery_subscribers_v2 {

  join: bigquery_conversion_model_addwatchlist {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.customer_id} = ${bigquery_conversion_model_addwatchlist.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_error {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.customer_id} = ${bigquery_conversion_model_error.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_firstplay {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.customer_id} = ${bigquery_conversion_model_firstplay.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_removewatchlist {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.customer_id} = ${bigquery_conversion_model_removewatchlist.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_timeupdate {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.customer_id} = ${bigquery_conversion_model_timeupdate.user_id};;
    relationship: one_to_one
  }

  join: bigquery_conversion_model_view {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.customer_id} = ${bigquery_conversion_model_view.user_id};;
    relationship: one_to_one
  }

  join: bigquery_delighted_survey_question_answered {
    type: left_outer
    sql_on:  ${bigquery_subscribers_v2.customer_id} = safe_cast(${bigquery_delighted_survey_question_answered.user_id} as int64);;
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
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_all_firstplay.user_id} AS INT64);;
    relationship: one_to_one
  }
  join: bigquery_delighted_survey_question_answered {
    type: left_outer
    sql_on: ${bigquery_delighted_survey_question_answered.user_id} = ${bigquery_derived_all_firstplay.user_id};;
    relationship: one_to_one
  }
  join: future_purchase_prediction {
    relationship: one_to_one
    sql_on: ${bigquery_derived_all_firstplay.user_id} = ${future_purchase_prediction.customer_id} ;;
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
    sql_on: ${bigquery_pixel_api_email_opened.user_id} = ${bigquery_http_api_purchase_event.user_id} AND (DATE_DIFF(${bigquery_http_api_purchase_event.received_date}, ${bigquery_pixel_api_email_opened.received_date}, DAY) <= 15);;
    #Attribution window of 15 Days
    relationship: many_to_many
  }


  join: bigquery_conversions {
    type: inner
    sql_on: ${bigquery_http_api_purchase_event.user_id} != ${bigquery_conversions.user_id};;
    relationship: many_to_many
  }




}


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
