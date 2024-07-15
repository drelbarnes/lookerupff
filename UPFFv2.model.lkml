connection: "upff"

# include all the views
# include views
include: "ios_users.view"
include: "javascript_users.view"
include: "javascript_identifies.view"
include: "android_users.view"
include: "javascript_subscribed.view"
include: "javascript_users.view"
include: "javascript_play.view"
include: "titles_id_mapping.view"
include: "daily_spend.view"
include: "app_installers.view.lkml"
include: "timeupdate.view.lkml"
include: "daily_cpa.view.lkml"
include: "mtd_free_trials.view.lkml"
include: "daily_spend_v2.view"
include: "mvpds.view.lkml"
include: "redshift_pixel_api_email_opened.view.lkml"
include: "redshift_php_get_churn_survey.view.lkml"
include: "redshift_php_get_trialist_survey.view.lkml"
include: "redshift_facebook_ads.view.lkml"
include: "redshift_facebook_campaigns.view.lkml"
include: "redshift_facebook_insights.view.lkml"
include: "redshift_google_campaign_performance_reports.view.lkml"
include: "redshift_google_campaigns.view.lkml"
include: "redshift_google_ad_performance_reports.view.lkml"
include: "redshift_google_ads.view.lkml"
include: "redshift_google_ad_groups.view.lkml"
include: "redshift_marketing_installs.view.lkml"
include: "redshift_ribbow_agency_fee.view.lkml"
include: "redshift_exec_summary_metrics.view.lkml"
include: "analytics_v2.view"
include: "analytics_v3.view.lkml"
include: "/views/redshift_views/vimeo_ott_webhook_events.view.lkml"
include: "/views/redshift_views/vimeo_ott_subscriber_events.view.lkml"
include: "/views/redshift_views/vimeo_ott_customer_record.view.lkml"
include: "/views/redshift_views/vimeo_ott_customer_record_analytics.view.lkml"
include: "analytics_upff_targets.view.lkml"
include: "mailchimp_email_campaigns.view"
include: "delighted_survey_question_answered.view"
include: "/views/customer_file_subscriber_counts.view.lkml"
include: "/views/appstoreconnect_sub_counts.view.lkml"
include: "/views/chargebee_event_mapping/gaithertvplus_analytics.view.lkml"
include: "/views/chargebee_event_mapping/gaithertvplus_app_installers.view.lkml"
include: "/views/chargebee_event_mapping/chargebee_webhook_events.view.lkml"
include: "/views/redshift_views/upff_webhook_events.view.lkml"
include: "/views/chargebee_event_mapping/chargebee_vimeo_ott_id_mapping.view.lkml"

#redshift allfirstplay explores

include: "redshift_allfirstplay_javascript_video_content_playing.view.lkml"
explore: redshift_allfirstplay_javascript_video_content_playing {
  label: "Redshift Javascript VCP"
}

include: "redshift_allfirstplay_ios_video_content_playing.view.lkml"
explore: redshift_allfirstplay_ios_video_content_playing {
  label: "Redshift IOS VCP"
}

include: "redshift_allfirstplay_android_video_content_playing.view.lkml"
explore: redshift_allfirstplay_android_video_content_playing {
  label: "Redshift Android VCP"
}

include: "redshift_allfirstplay_amazon_video_content_playing.view.lkml"
explore: redshift_allfirstplay_amazon_video_content_playing {
  label: "Redshift Amazon VCP"
}

include: "redshift_allfirstplay_roku_video_content_playing.view.lkml"
explore: redshift_allfirstplay_roku_video_content_playing {
  label: "Redshift Roku VCP"
}

include: "redshift_allfirst_play_q1_ios_firstplay.view.lkml"
explore: redshift_allfirst_play_q1_ios_firstplay {
  label: "Redshift IOS FP"
}

include: "redshift_allfirst_play_q2_android_firstplay.view.lkml"
explore: redshift_allfirst_play_q2_android_firstplay {
  label: "Reshift Android FP"
}

include: "redshift_allfirst_play_p0.view.lkml"
explore: redshift_allfirst_play_p0 {
  label: "Redshift Allfirstplay"
}

include: "redshift_allfirst_play_p1_less_granular.view.lkml"
explore: redshift_allfirst_play_p1_less_granular {
  label: "Redshift Plays Less Granular"
}

include: "redshift_allfirst_play_r0_upff_webhook_events.view.lkml"
explore: redshift_allfirst_play_r0_upff_webhook_events {
  label: "Redshift UPFF Transactional Data"
}

datagroup: redshift_upff_datagroup {
  description: "Datagroup for Allfirstplay and Timeupdate. Triggers twice per day"
  sql_trigger: SELECT FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP at time zone 'America/New_York') / 86400) ;;
  max_cache_age: "12 hours"
}

#end redshift allfirstplay

explore: redshift_exec_summary_metrics {
  label: "Exec Summary Metrics"
}

explore: redshift_ribbow_agency_fee {}

explore: redshift_marketing_installs {}

explore: redshift_facebook_insights {
  join: redshift_facebook_ads {
    type: inner
    sql_on: ${redshift_facebook_insights.ad_id}=${redshift_facebook_ads.id} ;;
    relationship: many_to_one
  }
  join: redshift_facebook_campaigns {
    type: inner
    sql_on: ${redshift_facebook_ads.campaign_id}=${redshift_facebook_campaigns.id} ;;
    relationship: many_to_one
  }
}

explore: redshift_google_campaign_performance_reports {
  join: redshift_google_campaigns {
    type: inner
    sql_on: ${redshift_google_campaign_performance_reports.campaign_id}=${redshift_google_campaigns.id} ;;
    relationship: many_to_one
  }
}

explore: redshift_google_ad_performance_reports {
  join: redshift_google_ads {
    type: inner
    sql_on: ${redshift_google_ad_performance_reports.ad_id}=${redshift_google_ads.id} ;;
    relationship: many_to_one
  }
  join: redshift_google_ad_groups {
    type: inner
    sql_on: ${redshift_google_ad_groups.id}=${redshift_google_ads.ad_group_id} ;;
    relationship: one_to_one
  }
  join: redshift_google_campaigns {
    type: inner
    sql_on: ${redshift_google_ad_groups.campaign_id}=${redshift_google_campaigns.id} ;;
    relationship: one_to_one
  }
}

explore: redshift_php_get_churn_survey {
  join: http_api_purchase_event {
    type: left_outer
    sql_on: ${http_api_purchase_event.user_id}=${redshift_php_get_churn_survey.user_id} ;;
    relationship: one_to_many
}}
explore: redshift_php_get_trialist_survey {
  join: http_api_purchase_event {
    type: left_outer
    sql_on: ${http_api_purchase_event.user_id}=${redshift_php_get_trialist_survey.user_id} ;;
    relationship: one_to_many
  }
}

explore: mvpds {}
explore: daily_spend_v2 {}

explore: mtd_free_trials{}

explore: daily_cpa {}


datagroup: upff_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "6 hour"
}

  datagroup: upff_event_processing {
    description: "Datagroup for UPFF Webhooks. Triggers once per day at 3am"
    sql_trigger: SELECT FLOOR((EXTRACT(epoch from GETDATE()) - 60*60*3)/(60*60*24)) ;;
    max_cache_age: "5 minutes"
  }

datagroup: upff_customer_file_reporting {
  description: "Datagroup for UPFF Customer File PDTs. Triggers once per day at 8:15am"
  sql_trigger: SELECT FLOOR((EXTRACT(epoch from GETDATE()) - 60*60*8.25)/(60*60*24)) ;;
  max_cache_age: "5 minutes"
}

datagroup: upff_acquisition_reporting {
  description: "Datagroup for UPFF Acquisition PDTs. Triggers once per day at 10:15am"
  sql_trigger: SELECT FLOOR((EXTRACT(epoch from GETDATE()) - 60*60*10.25)/(60*60*24)) ;;
  max_cache_age: "5 minutes"
}

include: "ios_application_installed.view"
include: "ios_signupstarted.view"

explore: timeupdate {}

explore: daily_spend {
  persist_with: upff_acquisition_reporting
}

explore: application_installed{
  join: ios_signupstarted {
    type:  left_outer
    sql_on: ${application_installed.anonymous_id} = ${ios_signupstarted.anonymous_id} ;;
    relationship: one_to_one
  }
}

explore: customer_file_subscriber_counts {
  persist_with: upff_customer_file_reporting
}

explore: appstoreconnect_sub_counts {
  persist_with: upff_customer_file_reporting
}

explore: analytics_v2 {
  persist_with: upff_acquisition_reporting
  join: mailchimp_email_campaigns {
    type:  inner
    sql_on: ${mailchimp_email_campaigns.campaign_date} = ${analytics_v2.timestamp_date};;
    relationship: one_to_one
  }
  join: daily_spend_v2 {
    type: inner
    sql_on: ${analytics_v2.timestamp_date}=${daily_spend_v2.timestamp_date} ;;
    relationship: one_to_one
  }
}

explore: analytics_v3 {
  join: analytics_upff_targets {
    type: left_outer
    sql_on: ${analytics_v3.timestamp_month} = ${analytics_upff_targets.month} ;;
    relationship: many_to_one
  }
  join: vimeo_ott_customer_record_analytics {
    type: left_outer
    sql_on: ${analytics_v3.datestamp} = ${vimeo_ott_customer_record_analytics.datestamp} ;;
    relationship: one_to_many
  }
}

explore: vimeo_ott_webhook_events {}

explore: vimeo_ott_subscriber_events {}

explore: vimeo_ott_customer_record {}

explore: vimeo_ott_customer_record_analytics {}

explore: gaithertvplus_analytics {}
explore: gaithertvplus_app_installers {}

explore: chargebee_webhook_events {}
explore: chargebee_vimeo_ott_id_mapping {}
explore: upff_webhook_events {
  label: "Redshift UPFF Webhook Events"
}

include: "javascript_subscribed.view"
explore: subscribed {}

include: "customers.view"
include: "customers_analytics.view"


include: "customers_churn_reasons_aggregated.view"
include: "customers_churn_custom_reasons.view"
include: "http_api_purchase_event.view"
include: "http_api_users.view"
include: "heartlandia.view"
include: "Lifetime_Value.view"
include: "churn_texts.view"
include: "LTV_CPA.view"
include: "customer_churn_percent.view"
include:  "ios_play.view"

explore: churn_reasons_aggregated {}
explore: churn_custom_reasons {}

explore: http_api_purchase_event {}
explore: http_api_users {}
explore: ios_identifies {}
explore: heartlandia {}

explore: lifetime_value {}
explore: churn_texts {}
explore: ltv_cpa {
  persist_with: upff_acquisition_reporting
}
explore: customer_churn_percent {}

explore: ios_play {}
explore: javascript_play {}


include: "javascript_users.view"
# Web Suscriber Plays
explore: javascript_users {
  label: "Web Subscriber Video ID"

  join: javascript_play {
    type:  inner
    sql_on: ${javascript_users.id} = ${javascript_play.user_id} ;;
    relationship: one_to_one
  }

}

include: "javascript_timeupdate.view"
include: "ios_timeupdate.view"
include: "android_timeupdate.view"
include: "javascript_authentication.view"
include: "javascript_derived_timeupdate.view"
include: "derived_marketing_attribution.view"
include: "ios_branch_install.view"
include: "ios_branch_open.view"
include: "ios_branch_reinstall.view"
include: "ios_identifies.view"
include: "android_branch_install.view"
include: "android_branch_reinstall.view"
include: "derived_subscriber_platform_total.view"
include: "mvpd_subs.view"
include: "mtd_revenue.view"
include: "upff_linear_ratings.view"
include: "uptv_daily_day_part.view"
include: "uptv_daily_key_demo.view"
include: "top_play.view"
include: "campaign_wicket_export.view"
include: "svod_titles.view"
include:  "/views/**/cine_romantico.view"

explore: titles {}
explore: mvpd_subs {}
explore: mtd_revenue {}
explore: upff_linear_ratings {}
explore: uptv_daily_key_demo {}
explore: uptv_daily_day_part {}
explore: top_play {}
explore: campaign_wicket_export {}
explore: cine_romantico {}
