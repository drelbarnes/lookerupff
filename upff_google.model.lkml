connection: "google_bigquery_db"

include: "bigquery_subscribers_timeupdate.view.lkml"
include: "bigquery_derived_timeupdate.view.lkml"
include: "bigquery_derived_views.view.lkml"
include: "bigquery_derived_all_firstplay.view.lkml"
include: "bigquery_derived_addwatchlist.view.lkml"
include: "bigquery_delighted_survey_question_answered.view.lkml"
include: "bigquery_subscribers.view.lkml"
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
include: "bigquery_allfirstplay_v2.view.lkml"
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
include: "bigquery_javascript_firstplay.view.lkml"
include: "bigquery_users.view.lkml"
include: "monthly_platform_user_count.view.lkml"
include: "bigquery_javascript_conversion.view.lkml"
include: "bigquery_javascript_pages.view.lkml"
include: "bigquery_javascript_users.view.lkml"
include: "bigquery_free_to_paid.view.lkml"
include: "bigquery_subscribers_v3.view.lkml"
include: "bigquery_churn_cohorts.view.lkml"
include: "bigquery_propensity_score.view.lkml"
include: "bigquery_looker_get_app_installs.view.lkml"
include: "bigquery_looker_get_app_reinstalls.view.lkml"
include: "bigquery_looker_get_clicks.view.lkml"
include: "churn_cohorts.view.lkml"
include: "bigquery_active_users.view.lkml"
include: "bigquery_churn_cohorts_v2.view.lkml"
include: "mysql_roku_firstplays.view.lkml"
include: "bigquery_purchase_event.view.lkml"
include: "bigquery_app_installs_by_platform.view.lkml"
include: "bigquery_http_api_roku_firstplay.view.lkml"
include: "roku_churn_segments.view.lkml"
include: "bigquery_python_users.view.lkml"
include: "bigquery_attribution.view.lkml"
include: "bigquery_get_titles.view.lkml"
include: "bigquery_get_title_category_items.view.lkml"
include: "bigquery_get_title_categories.view.lkml"
include: "survey_file.view.lkml"
include: "get_churn_survey.view.lkml"
include: "gender.view.lkml"
include: "facebook.view.lkml"
include: "marketing_site_pages.view.lkml"
include: "bigquery_marketing_installs.view.lkml"
include: "bigquery_all_conversions.view.lkml"
include: "bigquery_javascript_all_page_views.view.lkml"
include: "bigquery_get_user_on_email_list.view.lkml"
include: "gender.view.lkml"
include: "bigquery_heartland_viewer.view.lkml"
include: "bigquery_promoted_titles.view.lkml"
include: "bigquery_vimeott_webinar_ads.view.lkml"
include: "derived_redshift_add_watchlist.view.lkml"
include: "bigquery_wicket_marketing_cost.view.lkml"
include: "bigquery_vimeo_ott_customers.view.lkml"
include: "bigquery_vimeo_ott_customers_oct_2019.view.lkml"
include: "bigquery_vimeo_ott_customers_nov_2019.view.lkml"
include: "bigquery_vimeo_ott_customers_dec_2019.view.lkml"
include: "bigquery_involuntary_churn.view.lkml"
include: "bigquery_personas_v2.view.lkml"
include: "bigquery_personas_cluster_analysis.view.lkml"
include: "new_video_release.view.lkml"
include: "customer_segmentation.view.lkml"
include: "bigquery_platform_conversions.view.lkml"
include: "bigquery_churn_by_platform.view.lkml"
include: "op_uplift.view.lkml"
include: "op_uplift_registrations.view.lkml"
include: "sat.view.lkml"
include: "bigquery_ribbow_audiences.view.lkml"
include: "bigquery_fox_promo.view.lkml"
include: "vimeo_ott_metadata.view.lkml"
include: "metadata_live_grid.view.lkml"
include: "bigquery_annual_subs.view.lkml"
include: "bigquery_annual_churn.view.lkml"
include: "promos.view.lkml"
include: "customer_frequency.view.lkml"
include: "bigquery_video_content_playing_by_source.view.lkml"
include: "users.view.lkml"
include: "annual_kpis.view.lkml"
include: "bigquery_ribbon.view.lkml"
include: "bigquery_ribbon_plays.view.lkml"
include: "bigquery_utm_web_visits.view.lkml"
include: "bigquery_monthly_to_annual_conversions.view.lkml"
include: "bigquery_email_churn.view.lkml"
include: "bigquery_get_mailchimp_campaigns.view.lkml"
include: "bigquery_tickets.view.lkml"
include: "bigquery_push.view.lkml"
include: "bigquery_get_mailchimp_campaigns.view.lkml"
include: "bigquery_zendesk.view.lkml"
include: "bigquery_email_opens_set_cancels.view.lkml"
include: "bigquery_push_notification.view.lkml"
include: "promos1q21.view.lkml"
include: "bigquery_mobile_installs.view.lkml"
include: "bigquery_php_get_email_campaigns.view.lkml"
include: "bigquery_allfirstplay_legacy.view.lkml"
include: "bigquery_timeupdate_legacy.view.lkml"

include: "most_recent_purchase_events.view"
include: "max_churn_score.view"
include: "retention.view.lkml"

include: "bigquery_http_api_purchase_event_hubspot.view.lkml"
include: "/views/customer_product_set_cancellation.view.lkml"
include: "/views/hubspot_email_campaigns.view.lkml"
include: "/views/hubspot_email_events.view.lkml"
include: "bigquery_hubspot_email_sends.view.lkml"

include: "/views/hubspot_contacts.view.lkml"
include: "/views/identifies.view.lkml"
include: "validate_dunning.view.lkml"
include: "update_topic_hubspot.view.lkml"
include: "/views/purchase_event.view.lkml"
include: "/views/form_submissions.view.lkml"
include: "bigquery_identity_resolution.view.lkml"
include: "bigquery_marketing_attribution.view.lkml"
include: "bigquery_order_completed.view.lkml"
include: "bigquery_custom_marketing_spend.view.lkml"
include: "other_marketing_spend.view.lkml"
include: "counties.view.lkml"

include: "/views/multi_touch_attribution.view.lkml"
include: "/views/multi_touch_attribution_web.view.lkml"
include: "/views/multi_touch_attribution_web_tof.view.lkml"
include: "/views/identity_resolution.view.lkml"
include: "/views/vimeo_user_identities.view.lkml"
include: "/views/subscriber_lifecycles.view.lkml"
include: "/views/vimeo_app_screens.view.lkml"
include: "/views/upff_web_pages.view.lkml"
include: "/views/vimeo_app_installs.view.lkml"
include: "/views/vimeo_webhook_events.view.lkml"
include: "/views/bigquery_views/upff_chargebee_webhook_events.view.lkml"
include: "/views/vimeo_active_users.view.lkml"
include: "/views/customer_record.view.lkml"
include: "/views/customer_record_v2.view.lkml"
include: "/views/customer_record_analytics.view.lkml"
include: "/views/customer_record_bundle_analytics.view.lkml"

include: "/views/bigquery_views/chargebee_vimeo_ott_id_mapping.view.lkml"
include: "/views/bigquery_views/upff_webhook_events.view.lkml"
include: "/views/bigquery_views/upff_page_events.view.lkml"
include: "/views/bigquery_views/upff_web_sessions.view.lkml"
include: "/views/bigquery_views/upff_order_completed_events.view.lkml"
include: "/views/bigquery_views/ios_app_events.view.lkml"
include: "/views/bigquery_views/upff_ios_sessions.view.lkml"
include: "/views/bigquery_views/android_app_events.view.lkml"
include: "/views/bigquery_views/upff_android_sessions.view.lkml"
include: "/views/bigquery_views/upff_web_event_processing.view.lkml"
include:"/views/bigquery_views/upff_ios_event_processing.view.lkml"
include: "/views/bigquery_views/upff_android_event_processing.view.lkml"
include: "/views/bigquery_views/upff_web_attribution.view.lkml"
include: "/views/bigquery_views/upff_ios_attribution.view.lkml"
include: "/views/bigquery_views/upff_android_attribution.view.lkml"
include: "/views/bigquery_views/upff_multi_platform_attribution.view.lkml"
include: "/views/bigquery_views/cross_device_multi_channel_attribution/branch_events.view.lkml"

include: "allfirstplay.view.lkml"
include: "ticket_comments.view.lkml"
include: "heartland_analysis.view.lkml"
include: "xmas_movies.view.lkml"
include: "wishlist_p1.view.lkml"
include: "wishlist_p2.view.lkml"
include: "search_and_discovery.view.lkml"
include: "search_and_discovery2.view.lkml"
include: "hudson_rex_s2_e19.view.lkml"
include: "research_active_users.view.lkml"
include: "user_play_history.view.lkml"
include: "first_n_plays.view.lkml"
include: "first_n_plays_v3.view.lkml"
include: "popular_content_by_series.view.lkml"
include: "series_title_valuation.view.lkml"
include: "bigquery_viewing_habits.view.lkml"
include: "heartland_s16_movie_views.view.lkml"
include: "heartland_s16_series_views.view.lkml"
include: "heartland_s16_tenure_analysis.view.lkml"
include: "set_cancellations.view.lkml"
include: "/views/bigquery_views/bigquery_get_most_recent_titles.view.lkml"
include: "bigquery_meta_offline_events.view.lkml"
include: "svod_titles_index.view.lkml"
include: "svod_sub_counts.view.lkml"
include: "svod_view_counts.view.lkml"
include: "svod_content_type.view.lkml"
include: "first_n_plays_v2.view.lkml"
include: "minutes_distribution_by_title.view.lkml"
include: "n_day_views.view.lkml"
include: "premier_scorecard.view.lkml"

include: "/views/uptv_web/uptv_page_events.view.lkml"
include: "/views/uptv_web/uptv_web_sessions.view.lkml"
include: "/views/my_uptvapp/my_uptvapp_installs.view.lkml"
include: "/views/my_uptvapp/my_uptvapp_activity.view.lkml"
include: "/views/my_uptvapp/my_uptvapp_metrics.view.lkml"
include: "/views/my_uptvapp/my_uptvapp_identifies.view.lkml"
include: "/views/my_uptvapp/my_uptvapp_signups.view.lkml"
include: "/views/aspire_tvapp/my_aspireapp_installs.view.lkml"
include: "/views/aspire_tvapp/my_aspireapp_signups.view.lkml"
include: "/views/aspiretv_web/aspiretv_page_events.view.lkml"
include: "/views/aspiretv_web/aspiretv_web_sessions.view.lkml"
include: "/views/aspire_tvapp/my_aspireapp_activity.view.lkml"

# DTC App reporting #

include: "/views/bigquery_views/app_store_connect_aggregate.view.lkml"

# Brightcove Mapping #

include: "/views/bigquery_views/brightcove_revenue_events.view.lkml"
include: "/views/bigquery_views/brightcove_subscription_events.view.lkml"
include: "/views/bigquery_views/brightcove_product_id_mapping.view.lkml"
# include: "/views/chargebee_event_mapping/chargebee_webhook_events.view.lkml"
# include: "/views/chargebee_event_mapping/chargebee_analytics.view.lkml"
include: "/views/chargebee_event_mapping/gtv_vimeo_webhook_events.view.lkml"
include: "/views/bigquery_views/minno_webhook_events.view.lkml"
include: "/views/bigquery_views/gtv_webhook_events.view.lkml"

# Test Views #

include: "/views/testing_views/hubspot_bogo_testing.view.lkml"

# Ad Partners #

include: "/views/bigquery_views/radiant_reporting.view.lkml"

# Chargebee #

include: "/views/Chargebee/chargebee_subscriptions.view.lkml"

# Datagroups for PDT Triggers #

datagroup: upff_daily_refresh_datagroup {
  max_cache_age: "24 hours"
  description: "Datagroup for Identity Resolution and Attribution PDTs. Triggers once per day"
  sql_trigger: SELECT CURRENT_DATE() ;;
}

datagroup: upff_analytics_datagroup {
  max_cache_age: "12 hours"
  description: "Datagroup for Allfirstplay and Timeupdate. Triggers twice per day"
  sql_trigger: SELECT CURRENT_DATE() ;;
}

datagroup: chargebee_reporting {
  description: "Datagroup for Chargebee PDTs. Triggers once per day at 8:15am"
  sql_trigger: SELECT CAST(FORMAT_TIMESTAMP('%F', TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -495 MINUTE)) AS STRING) ;;
  max_cache_age: "5 minutes"
}

# Explores #

# explore: chargebee_webhook_events {
#   label: "Chargebee Webhook Events"
# }

# explore: chargebee_analytics {
#   label: "Chargebee Analytics"
# }

explore: chargebee_subscriptions {
  label: "Chargebee Subscriptions"
}

explore: radiant_reporting {
  label: "Radiant Reporting"
}

explore: gtv_webhook_events {
  label: "GTV+ Webhook Events"
}

explore: minno_webhook_events {
  label: "Minno Webhook Events"
}

explore: brightcove_subscription_events {
  label: "Brightcove Subscription Events"
  join: brightcove_revenue_events {
    type: left_outer
    sql_on: ${brightcove_subscription_events.subscription_id}=${brightcove_revenue_events.subscription_id};;
    relationship: one_to_many
  }
}

explore: brightcove_revenue_events {
  label: "Brightcove Revenue Events"
  join: brightcove_subscription_events {
    type: left_outer
    sql_on: ${brightcove_revenue_events.subscription_id}=${brightcove_subscription_events.subscription_id} ;;
    relationship: many_to_one
  }
}

explore: brightcove_product_id_mapping {
  label: "Brightcove Product ID Mapping"
}

explore: chargebee_vimeo_ott_id_mapping {
  label: "Chargebee - Vimeo OTT ID Mapping"
}

explore: aspiretv_page_events {
  label: "aspiretv_page_events"
}

explore: aspiretv_web_sessions {
  label: "aspiretv_web_sessions"
}

explore: my_aspireapp_activity {
  label: "my aspireapp activity"
}

explore: my_aspireapp_signups {
  label: "my_aspireapp_signups"
}

explore: my_aspireapp_installs {
  label: "My aspiretv Installs"
  join: my_aspireapp_signups {
    type: left_outer
    sql_on: ${my_aspireapp_installs.anonymous_id}=${my_aspireapp_signups.anonymous_id} ;;
    relationship: one_to_one
  }
}

explore: uptv_web_sessions {
  label: "UPtv Web Sessions"
}

explore: uptv_page_events {
  label: "UPtv Page Events"
}

explore: my_uptvapp_installs {
  label: "My UPtv Installs"
  join: my_uptvapp_signups {
    type: left_outer
    sql_on: ${my_uptvapp_installs.anonymous_id}=${my_uptvapp_signups.anonymous_id} ;;
    relationship: one_to_one
  }
}

explore: my_uptvapp_signups {
  label: "My UPtv Signups"
}

explore: my_uptvapp_activity {
  label: "My UPtv Activity"
}

explore: my_uptvapp_metrics {
  label: "My UPtv Metrics"
}

explore: my_uptvapp_identifies {
  label: "My UPtv Identifies"
}

explore: bigquery_meta_offline_events {
  label: "Meta Offline Events"
}

explore: bigquery_get_most_recent_titles {
  label: "bigquery_get_most_recent_titles"
}

explore: bigquery_allfirstplay_legacy {
  label: "Bigquery Allfirstplay Legacy"
}

explore: bigquery_timeupdate_legacy {
  label: "Bigquery Timeupdate Legacy"
}

explore: app_store_connect_aggregate {
  label: "App Store Connect Subs"
}

explore: user_play_history {
  label: "User Play History"
  always_filter: {
    filters: [user_id: "22620729"]
    }
}

explore: set_cancellations {
  label: "Set Cancellations"
}

explore: heartland_s16_tenure_analysis {
  label: "Heartland S16 Tenure"
}
explore: heartland_s16_movie_views {
  label: "Heartland S16 Viewers"
}

explore: heartland_s16_series_views {
  label: "Heartland S16 Viewers 2"
}

explore: svod_sub_counts {
  label: "SVOD Sub Counts"
}

explore: svod_view_counts {
  label: "SVOD View Counts"
}

explore: svod_content_type {
  label: "SVOD Content Type"
}

explore: series_title_valuation {
  label: "Series Title Valuation"
}

explore: popular_content_by_series {
  label: "Popular Content by Series"
}

explore: bigquery_viewing_habits {
  label: "Viewing Habits"
}

explore: first_n_plays {
  label: "First N Plays"
}

explore: first_n_plays_v2 {
  label: "First N Plays V2"
}

explore: first_n_plays_v3 {
  label: "First N Plays V3"
}

explore: svod_titles_index {
  label: "SVOD Titles Index"
}

explore: research_active_users {
  label: "Research Active Users"
}

explore: hudson_rex_s2_e19 {
label: "Hudson & Rex S2 E19 Completers"
}

explore: search_and_discovery {
  label: "Search"
}

explore: search_and_discovery2 {
  label: "Results"
}

explore: wishlist_p1 {
  label: "Wishlist P1"
}

explore: wishlist_p2 {
  label: "Wishlist P2"
}

explore: xmas_movies {
  label: "Christmas Movies"
}

explore: heartland_analysis {
  label: "Heartland Analysis"
}

explore: ticket_comments {
  label: "Ticket Comments"
}

explore: allfirstplay {
  label: "Allfirstplay"
}

explore: minutes_distribution_by_title {
  label: "Minutes Distribution by Title"
}

explore: n_day_views {
  label: "Nth Day Views Benchmark"
}

explore: premier_scorecard {
  label: "Premier Scorecard"
}

explore: upff_chargebee_webhook_events {
  label: "UPFF Chargebee Webhook Events"
}

explore: upff_webhook_events {
  label: "UPFF Webhook Events"
}

explore: upff_page_events {
  label: "UPFF Page Events"
}

explore: upff_web_sessions {
  label: "UPFF Web Sessions"
}

explore: vimeo_webhook_events {
  label: "Vimeo Webhook Events"
}

explore: upff_order_completed_events {
  label: "UPFF Order Completed Events"
}

explore: ios_app_events {
  label: "iOS App Events"
}

explore: android_app_events {
  label: "Android App Events"
}

explore: upff_ios_sessions {
  label: "UPFF iOS Sessions"
}

explore: upff_android_sessions {
  label: "UPFF Android Sessions"
}

explore: upff_web_event_processing {
  label: "UPFF Web Event Processing"
}

explore: upff_ios_event_processing {
  label: "UPFF iOS Event Processing"
}

explore: upff_android_event_processing {
  label: "UPFF Android Event Processing"
}

explore: upff_web_attribution {
  label: "UPFF Web Attribution"
}

explore: upff_ios_attribution {
  label: "UPFF iOS Attribution"
}

explore: upff_android_attribution {
  label: "UPFF Android Attribution"
}

explore: upff_multi_platform_attribution {
  label: "UPFF Multi Platform Attribution"
  join: upff_web_sessions {
    type: left_outer
    sql_on: ${upff_multi_platform_attribution.session_id} = ${upff_web_sessions.session_id} ;;
    relationship: many_to_many
  }
}

explore: customer_record {
  label: "Customer Record"
}

explore: customer_record_v2 {
  label: "Customer Record V2"
}


explore: customer_record_analytics {
  label: "Customer Record Analytics"
}

explore: customer_record_bundle_analytics {
  label: "Customer Record Bundle Analytics"
}

explore: vimeo_active_users {
  label: "Vimeo Active Users"
}

explore: subscriber_lifecycles  {
  label: "Subscriber Lifecycles"
}

explore: multi_touch_attribution {
  label: "Multi-touch Attribution (Dev)"
}

explore: vimeo_user_identities {
  label: "Vimeo User Identities"
}

explore: branch_events {
  label: "Branch Events"
}

explore: funnel_performance {
  view_name: vimeo_user_identities
  label: "Funnel Performance"

  join: upff_web_pages {
    type: full_outer
    sql_on: ${vimeo_user_identities.anonymous_id} = ${upff_web_pages.anonymous_id} ;;
    relationship: many_to_many
  }

  join: subscriber_lifecycles {
    type: full_outer
    sql_on: ${vimeo_user_identities.user_id} = ${subscriber_lifecycles.user_id} ;;
    relationship: many_to_many
  }

  join: vimeo_active_users {
    type: left_outer
    sql_on: ${vimeo_user_identities.user_id} = ${vimeo_active_users.user_id} ;;
    relationship: many_to_many
  }

  join: vimeo_app_screens {
    type: full_outer
    sql_on: ${vimeo_user_identities.device_id} = ${vimeo_app_screens.device_id} ;;
    relationship: many_to_many
  }

  join: vimeo_app_installs {
    type: full_outer
    sql_on: ${vimeo_user_identities.device_id} = ${vimeo_app_installs.device_id} ;;
    relationship: many_to_many
  }

  join: multi_touch_attribution_web {
    type: full_outer
    sql_on: ${vimeo_user_identities.anonymous_id} = ${multi_touch_attribution_web.anonymous_id} ;;
    relationship: many_to_many
  }

  join: multi_touch_attribution_web_tof {
    type: full_outer
    sql_on: ${vimeo_user_identities.anonymous_id} = ${multi_touch_attribution_web_tof.anonymous_id} ;;
    relationship: many_to_many
  }

  join: customer_record {
    type: full_outer
    sql_on: ${vimeo_user_identities.user_id} = ${customer_record.user_id} ;;
    relationship: many_to_many
  }

  join: customer_record_analytics {
    type: left_outer
    sql_on: ${customer_record.date_date} = ${customer_record_analytics.date_date}
      and ${customer_record.platform} = ${customer_record_analytics.platform}
      and ${customer_record.frequency} = ${customer_record_analytics.frequency} ;;
    relationship: many_to_many
  }

  join: upff_web_sessions {
    type: full_outer
    sql_on: ${vimeo_user_identities.anonymous_id}=${upff_web_sessions.anonymous_id};;
    relationship: many_to_many
  }
}

explore: identity_resolution {
  label: "Cross Promotion Performance"

  join: multi_touch_attribution {
    type: full_outer
    sql_on: ${identity_resolution.anonymous_id} = ${multi_touch_attribution.anonymous_id};;
    relationship: many_to_many
  }
  join: purchase_event {
    type:  left_outer
    sql_on:  ${purchase_event.user_id} = ${multi_touch_attribution.user_id};;
    relationship: many_to_one
  }

}

explore: hubspot_contacts {
  label: "Abandoned Cart Workflow"

  join: form_submissions {
    type: inner
    sql_on: ${form_submissions.id} = ${hubspot_contacts.form_submissions} ;;
    relationship: many_to_one
  }

}

explore: counties {
  label: "Geo Location"
}

explore: other_marketing_spend {
  label:"Other Marketing Spend"
}

explore: bigquery_custom_marketing_spend
{
  label: "Marketing Spend"
}

explore: bigquery_order_completed {
  label: "Order Completed"
}

explore:  bigquery_identity_resolution {
  label: "Marketing Attribution 2.0"
  join: bigquery_marketing_attribution {
    type: left_outer
    sql_on: ${bigquery_identity_resolution.anonymous_id} = ${bigquery_marketing_attribution.anonymous_id}
      ;;
    relationship: many_to_many
  }

  join: bigquery_custom_marketing_spend {
    type: left_outer
    sql_on: ${bigquery_identity_resolution.timestamp_date} = ${bigquery_custom_marketing_spend.timestamp} ;;
    relationship: many_to_one
  }
}

datagroup: purchase_event_datagroup {
  sql_trigger: SELECT max(status_date) FROM `up-faith-and-family-216419.http_api.purchase_event` ;;
  max_cache_age: "5 minutes"
  label: "New Purchase Event"
  description: "Triggered every hour, on the hour"
}

explore: purchase_event {}

explore: validate_dunning {}
explore: update_topic_hubspot {}

explore:  max_churn_score {}

explore:  most_recent_purchase_events {
  persist_with: purchase_event_datagroup
}

explore: cross_promotion {
  view_name: bigquery_identity_resolution
  join: bigquery_marketing_attribution {
    type: left_outer
    sql_on: ${bigquery_identity_resolution.anonymous_id} = ${bigquery_marketing_attribution.anonymous_id}
      ;;
    relationship: many_to_many
  }
  join: bigquery_custom_marketing_spend {
    type: left_outer
    sql_on: ${bigquery_identity_resolution.timestamp_date} = ${bigquery_custom_marketing_spend.timestamp} ;;
    relationship: many_to_one
  }
  join: purchase_event {
    type:  left_outer
    sql_on:  ${purchase_event.user_id} = ${bigquery_marketing_attribution.user_id};;
    relationship: many_to_one
  }
}



explore: retention {
  label: "Retention 11-18-21"
}

explore: bigquery_php_get_email_campaigns {
  label: "Email Campaigns (BigQuery)"
}

explore: bigquery_allfirstplay_v2 {
  label: "All First Play V2"
}

explore: bigquery_mobile_installs {}

explore: promos1q21 {}

explore: bigquery_push_notification {}

include: "bigquery_email_sends.view.lkml"

explore: bigquery_email_sends {
  label: "MailChimp Email Sends (User-level)"
}

include: "bigquery_titles.view.lkml"
explore: bigquery_titles {}

include: "bigquery_mvpd_subs.view.lkml"
explore: bigquery_mvpd_subs {}

include: "bigquery_mvpd_titles.view.lkml"
explore: bigquery_mvpd_titles {}

include: "svod_monthly_v2_dashboard.dashboard"

include: "mvpd_subs_gbc.view.lkml"
explore: mvpd_subs_gbc {}

include: "svod_titles_gbc.view.lkml"
explore: svod_titles_gbc {}

include: "svod_titles_general.view.lkml"
explore: svod_titles_general {}

explore: bigquery_email_opens_set_cancels {}

explore: bigquery_get_mailchimp_campaigns {
  join: bigquery_http_api_purchase_event {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${bigquery_get_mailchimp_campaigns.email}
      ;;
    relationship: many_to_many
  }
}

explore: bigquery_zendesk {
  join: bigquery_http_api_purchase_event {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${bigquery_zendesk.email}
      AND DATE_DIFF(${bigquery_zendesk.created_at_date},${bigquery_http_api_purchase_event.status_date},DAY)<31 ;;
    relationship: many_to_many
  }
  join: bigquery_annual_subs {
    type: left_outer
    sql_on: ${bigquery_zendesk.email}=${bigquery_annual_subs.email}
      AND DATE_DIFF(${bigquery_zendesk.created_at_date},${bigquery_annual_subs.status_date__date},DAY)<31;;
    relationship: many_to_many
  }
}

explore: bigquery_push {
  join: bigquery_http_api_purchase_event {
    type: left_outer
    sql_on:${bigquery_push.user_id}=${bigquery_http_api_purchase_event.user_id} and
      date_diff(${bigquery_http_api_purchase_event.status_date},${bigquery_push.timestamp_date},day)<31;;
    relationship: many_to_many
  }
  join: bigquery_allfirstplay {
    type: inner
    sql_on: ${bigquery_allfirstplay.user_id}=${bigquery_push.user_id} and
            ${bigquery_push.timestamp_date}<=${bigquery_allfirstplay.timestamp_date} and
            ${bigquery_push.timestamp_date}>=date_sub(${bigquery_allfirstplay.timestamp_date},interval 30 day);;
    relationship: many_to_many
  }
}

explore: bigquery_tickets {}

explore: bigquery_email_churn {
  join: bigquery_analytics {
    type: inner
    sql_on: ${bigquery_email_churn.status_date_date}=${bigquery_analytics.timestamp_date};;
    relationship: many_to_one
  }
}

explore: bigquery_monthly_to_annual_conversions {}

explore: bigquery_utm_web_visits {
  join: bigquery_javascript_conversion {
    type: left_outer
    sql_on: ${bigquery_utm_web_visits.anonymous_id}=${bigquery_javascript_conversion.anonymous_id} and ${bigquery_utm_web_visits.timestamp_date}=${bigquery_javascript_conversion.timestamp_date};;
    relationship: many_to_many
  }

  join: bigquery_http_api_purchase_event {
    type: left_outer
    sql_on: ${bigquery_javascript_conversion.user_id}=${bigquery_http_api_purchase_event.user_id} ;;
    relationship: many_to_many
  }

  join: bigquery_timeupdate {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.user_id}=${bigquery_timeupdate.user_id} and ${bigquery_timeupdate.timestamp_date}=${bigquery_utm_web_visits.timestamp_date} ;;
    relationship: many_to_many
  }
}

explore: bigquery_ribbon_plays {}

explore: bigquery_ribbon {}

explore: annual_kpis {}

explore: users {}
explore: bigquery_video_content_playing_by_source {}

explore: customer_frequency {}

explore: bigquery_annual_churn {
  join: bigquery_analytics {
    type: inner
    sql_on: date(${bigquery_annual_churn.status_date_date})=date(${bigquery_analytics.timestamp_date}) ;;
    relationship: one_to_one
  }
}

explore: bigquery_annual_subs {}

explore: metadata_live_grid {

}
explore: vimeo_ott_metadata{
}

explore: bigquery_fox_promo {}


explore: sat {}

explore: op_uplift_registrations {
  join: bigquery_http_api_purchase_event {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${op_uplift_registrations.email} and ${bigquery_http_api_purchase_event.status_date}>=${op_uplift_registrations.entry_date} ;;
    relationship: one_to_one
  }
}

explore: op_uplift {
  join: bigquery_http_api_purchase_event {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${op_uplift.email} and ${bigquery_http_api_purchase_event.status_date}>=${op_uplift.entry_date} ;;
    relationship: one_to_one
  }
}

explore: bigquery_churn_by_platform {}

explore: bigquery_platform_conversions {}

explore: customer_segmentation {}

explore: new_video_release {}

explore: bigquery_personas_v2 {}

explore: bigquery_involuntary_churn {}

explore: bigquery_vimeo_ott_customers_dec_2019 {

  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: ${bigquery_vimeo_ott_customers_dec_2019.customer_id} = SAFE_CAST(${bigquery_http_api_purchase_event.user_id} as INT64) ;;
    relationship: one_to_many
  }

}

explore: bigquery_vimeo_ott_customers_nov_2019 {

  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: ${bigquery_vimeo_ott_customers_nov_2019.customer_id} = SAFE_CAST(${bigquery_http_api_purchase_event.user_id} as INT64) ;;
    relationship: one_to_many
  }

}

explore: bigquery_vimeo_ott_customers_oct_2019 {

  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: ${bigquery_vimeo_ott_customers_oct_2019.customer_id} = SAFE_CAST(${bigquery_http_api_purchase_event.user_id} as INT64) ;;
    relationship: one_to_many
  }

}

explore: bigquery_vimeo_ott_customers {

  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: ${bigquery_vimeo_ott_customers.customer_id} = SAFE_CAST(${bigquery_http_api_purchase_event.user_id} as INT64);;
    relationship: one_to_many
  }

}

explore: bigquery_wicket_marketing_cost {}

explore: derived_redshift_add_watchlist {}

explore: bigquery_vimeott_webinar_ads{}


explore: bigquery_heartland_viewer {}


explore: bigquery_javascript_all_page_views {
  join: bigquery_javascript_conversion {
    type: left_outer
    sql_on: ${bigquery_javascript_all_page_views.anonymous_id}=${bigquery_javascript_conversion.anonymous_id} and ${bigquery_javascript_all_page_views.timestamp_date}=${bigquery_javascript_conversion.timestamp_date};;
    relationship: one_to_one
  }

  join: bigquery_http_api_purchase_event {
    type: left_outer
    sql_on: ${bigquery_javascript_conversion.user_id}=${bigquery_http_api_purchase_event.user_id} ;;
    relationship: one_to_one
  }
}

explore: bigquery_marketing_installs{
  join: bigquery_all_conversions {
    type: left_outer
    sql_on: ${bigquery_all_conversions.anonymous_id}=${bigquery_marketing_installs.anonymous_id} ;;
    relationship: one_to_one
  }
}

explore: bigquery_all_conversions {}

explore: marketing_site_pages {}

explore: facebook {}


explore: bigquery_http_api_purchase_event {

  join: bigquery_get_user_on_email_list {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email} = ${bigquery_get_user_on_email_list.email} ;;
    relationship: one_to_one
  }

  join: gender {
    type: left_outer
    sql_on: ${gender.name}=${bigquery_http_api_purchase_event.fname} ;;
    relationship: one_to_one
  }

  join: op_uplift {
    type: left_outer
    sql_on: ${op_uplift.email}=${bigquery_http_api_purchase_event.email} and ${bigquery_http_api_purchase_event.status_date}>=${op_uplift.entry_date};;
    relationship: one_to_one
  }

  join: bigquery_get_mailchimp_campaigns {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${bigquery_get_mailchimp_campaigns.email} and date_diff(${bigquery_http_api_purchase_event.status_date},${bigquery_get_mailchimp_campaigns.timestamp_date},day)<31;;
    relationship: one_to_one
  }

  join: bigquery_php_get_email_campaigns{
    type:  left_outer
    sql_on:  ${bigquery_php_get_email_campaigns.timestamp_date} = ${bigquery_get_mailchimp_campaigns.timestamp_date} ;;
    relationship: many_to_many
  }

  join: bigquery_push_notification {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.user_id}=${bigquery_push_notification.user_id} ;;
    relationship: one_to_one
  }

  join: bigquery_push {
    type: left_outer
    sql_on: ${bigquery_push.user_id}=${bigquery_http_api_purchase_event.user_id} and
      date_diff(${bigquery_http_api_purchase_event.status_date},${bigquery_push.timestamp_date},day)<31;;
    relationship: one_to_one  }

  join: bigquery_zendesk {
    type: left_outer
    sql_on: ${bigquery_zendesk.email}=${bigquery_http_api_purchase_event.email} and
      date_diff(${bigquery_http_api_purchase_event.status_date},${bigquery_zendesk.created_at_date},day)<31;;
    relationship: one_to_one  }

  join: bigquery_email_sends {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${bigquery_email_sends.email};;
    relationship: many_to_many
  }

  join: hubspot_email_events {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${hubspot_email_events.recipient} and date_diff(${bigquery_http_api_purchase_event.status_date},${hubspot_email_events.sent_by_created_date},day)<31;;
    relationship: one_to_one
  }
  join: bigquery_hubspot_email_sends {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${bigquery_hubspot_email_sends.email};;
    relationship: many_to_many
  }
  join: hubspot_email_campaigns{
    type:  left_outer
    sql_on:  ${hubspot_email_campaigns.last_processing_state_change_date} = ${hubspot_email_events.sent_by_created_date} ;;
    relationship: many_to_many
  }
}

explore: customer_product_set_cancellation {
  join: hubspot_email_events {
    type: left_outer
    sql_on: ${customer_product_set_cancellation.email}=${hubspot_email_events.recipient} and date_diff(${customer_product_set_cancellation.timestamp_date},${hubspot_email_events.sent_by_created_date},day)<31;;
    relationship: one_to_one
  }
  join: bigquery_hubspot_email_sends {
    type: left_outer
    sql_on: ${customer_product_set_cancellation.email}=${bigquery_hubspot_email_sends.email};;
    relationship: many_to_many
  }
  join: hubspot_email_campaigns{
    type:  left_outer
    sql_on:  ${hubspot_email_campaigns.last_processing_state_change_date} = ${hubspot_email_events.sent_by_created_date} ;;
    relationship: many_to_many
  }
}

explore: survey_file{
  join: bigquery_http_api_purchase_event {
    type: inner
    sql_on: cast(${survey_file.customer_id} as string)=cast(${bigquery_http_api_purchase_event.user_id} as string) ;;
    relationship: one_to_one
  }
  join: churn_prediction {
    type: inner
    sql_on: cast(${churn_prediction.customer_id} as string)=cast(${survey_file.customer_id} as string) ;;
    relationship: one_to_one
  }
  join: get_churn_survey {
    type: inner
    sql_on: cast(${survey_file.customer_id} as string)=cast(${get_churn_survey.user_id} as string) ;;
    relationship: one_to_one
  }
  join: bigquery_allfirstplay {
    type: inner
    sql_on: cast(${survey_file.customer_id} as string)=cast(${bigquery_allfirstplay.user_id} as string);;
    relationship: one_to_many
  }

  join: bigquery_email_sends {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${bigquery_email_sends.email};;
    relationship: many_to_many
  }
}

#f
explore: bigquery_get_title_category_items {
  join: bigquery_get_title_categories {
    type: inner
    sql_on: ${bigquery_get_title_categories.cat_id}=${bigquery_get_title_category_items.cat_id} ;;
    relationship: one_to_one
  }
  join: bigquery_get_titles {
    type: inner
    sql_on: ${bigquery_get_titles.collection}=${bigquery_get_title_category_items.name};;
    relationship: one_to_one
  }
  join: bigquery_allfirstplay {
    type: inner
    sql_on: ${bigquery_allfirstplay.video_id}=${bigquery_get_titles.video_id} ;;
    relationship: one_to_one
  }
}


explore: bigquery_attribution {}

explore: roku_churn_segments {}

explore: bigquery_app_installs_by_platform {}

include: "redshift_php_get_weekly_comments.view.lkml"

explore: mysql_roku_firstplays {}

explore: bigquery_churn_cohorts_v2 {}

explore: bigquery_active_users {}

explore: churn_cohorts {}

include: "bigquery_firebase_events_20190225.view.lkml"


explore: bigquery_churn_cohorts {}

explore: bigquery_analytics {
  join: bigquery_allfirstplay {
    type: inner
    sql_on: ${bigquery_allfirstplay.timestamp_date}=${bigquery_analytics.timestamp_date} ;;
    relationship: one_to_one
  }
  join: bigquery_email_churn {
    type: inner
    sql_on: ${bigquery_email_churn.status_date_date}=${bigquery_analytics.timestamp_date} ;;
    relationship: one_to_one
  }
}


# include: "bigquery_php_get_roku_firstplay.view.lkml"


include: "bigquery_http_api_get_roku_firstplay.view.lkml"

explore: monthly_platform_user_count {}

datagroup: upff_google_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "24 hour"
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
    type: inner
    sql_on: ${bigquery_timeupdate.user_id}=${bigquery_http_api_purchase_event.user_id} ;;
    relationship: one_to_one
  }
  join: bigquery_allfirstplay {
    type: inner
    sql_on: ${bigquery_allfirstplay.user_id}=${bigquery_timeupdate.user_id} and ${bigquery_allfirstplay.timestamp_date}=${bigquery_timeupdate.timestamp_date} and ${bigquery_allfirstplay.video_id}=${bigquery_timeupdate.video_id} ;;
    relationship:one_to_one}
}
explore: bigquery_topmovies {
  view_label: "Movie"
}
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

#   join: bigquery_conversion_model_firstplay {
#     type: left_outer
#     sql_on:  ${bigquery_subscribers_v2.user_id} = ${bigquery_conversion_model_firstplay.user_id};;
#     relationship: one_to_one
#   }

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

  join: bigquery_python_users {
    type:  left_outer
    sql_on:  ${bigquery_allfirstplay.anonymousId} = ${bigquery_python_users.id};;
    relationship: one_to_one
  }

  join: promos1q21 {
    type: inner
    sql_on: upper(${bigquery_allfirstplay.collection})=upper(${promos1q21.collection}) and date_diff(${bigquery_allfirstplay.timestamp_date},${promos1q21.date},day)<=7 and date_diff(${bigquery_allfirstplay.timestamp_date},${promos1q21.date},day)>=0;;
    relationship: many_to_one
  }

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
  join: gender {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.fname}=${gender.name} ;;
    relationship: many_to_one
  }
  join: bigquery_promoted_titles {
    type: inner
    sql_on: ${bigquery_allfirstplay.timestamp_day_of_week}=${bigquery_promoted_titles.timestamp_day_of_week} ;;
    relationship: one_to_one
  }
  join: users {
    type: left_outer
    sql_on: ${bigquery_allfirstplay.user_id}=${users.user_id} ;;
    relationship: many_to_one
  }

  join: bigquery_ribbow_audiences {
    type: left_outer
    sql_on: ${bigquery_allfirstplay.user_id}=${bigquery_ribbow_audiences.user_id} ;;
    relationship: many_to_one
  }
  join: promos {
    type: left_outer
    sql_on: ${bigquery_allfirstplay.video_id}=${promos.video_id} ;;
    relationship: many_to_one
  }

  join: bigquery_ribbon {
    type: left_outer
    sql_on: ${bigquery_ribbon.collection}=${bigquery_allfirstplay.collection};;
    relationship: many_to_many
  }

  join: bigquery_get_mailchimp_campaigns {
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.email}=${bigquery_get_mailchimp_campaigns.email};;
    relationship: one_to_many
  }

}

explore: bigquery_ribbow_audiences {}

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
    type: left_outer
    sql_on: ${bigquery_php_get_user_on_email_list.email} = ${bigquery_http_api_purchase_event.email};;
    relationship: one_to_many
  }

  join: bigquery_pixel_api_email_opened{
    type: left_outer
    sql_on: ${bigquery_http_api_purchase_event.user_id} = ${bigquery_pixel_api_email_opened.user_id};;
    relationship: many_to_many
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
explore: bigquery_propensity_score {

  join: future_purchase_prediction {
    type: left_outer
    sql_on: ${future_purchase_prediction.user_id} = ${bigquery_propensity_score.user_id};;
    relationship: one_to_many
  }

}

explore: bigquery_looker_get_clicks {

  join: bigquery_looker_get_app_installs {
    type: left_outer
    sql_on: ${bigquery_looker_get_clicks.received_date} = ${bigquery_looker_get_app_installs.received_date};;
    relationship: one_to_one
  }

  join: bigquery_looker_get_app_reinstalls {
    type: left_outer
    sql_on: ${bigquery_looker_get_clicks.received_date} = ${bigquery_looker_get_app_reinstalls.received_date};;
    relationship: one_to_one
  }

}

explore: redshift_php_get_weekly_comments {}
explore: bigquery_http_api_roku_firstplay{}

# Explores for testing pipelines #

explore: hubspot_bogo_testing {
  label: "HubSpot BOGO Testing"
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
