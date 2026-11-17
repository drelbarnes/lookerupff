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
include: "daily_spend_vw.view.lkml"
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
include: "/views/redshift_views/bundle_analytics.view.lkml"
include: "/views/UPFF_Vw/subscriber_data.view.lkml"
include: "/views/UPFF_Vw/chargebee_webhook.view.lkml"
include: "/views/UPFF_Vw/vimeo_webhook.view.lkml"
include: "/views/UPFF_Vw/free_trials.view.lkml"

#redshift upff datamart explores
include: "upff_datamart_views.view.lkml"
explore: upff_datamart_views {
  label: "UPFF Datamart Views"
}

include: "upff_datamart_customers.view.lkml"
explore: upff_datamart_customers {
  label: "UPFF Datamart Customers"
}

include: "upff_datamart_customer_events.view.lkml"
explore: upff_datamart_customer_events {
  label: "UPFF Datamart Customer Events"
}

include: "upff_datamart_subscriptions.view.lkml"
explore: upff_datamart_subscriptions {
  label: "UPFF Datamart Subscriptions"
}

include: "upff_heartland_fans.view.lkml"
explore: upff_heartland_fans {
  label: "UPFF Heartland Fans"
}

include: "upff_series_premier.view.lkml"
explore: upff_series_premier {
  label: "UPFF Series Premier"
}

include: "upff_movie_premier.view.lkml"
explore: upff_movie_premier {
  label: "UPFF Movie Premier"
}

include: "upff_series_overlap.view.lkml"
explore: upff_series_overlap {
  label: "UPFF Series Overlap"
}

include: "upff_series_dropoff.view.lkml"
explore: upff_series_dropoff {
  label: "UPFF Series Dropoff"
}

include: "upff_weekly_recap_quarterly_benchmarks.view.lkml"
explore: upff_weekly_recap_quarterly_benchmarks {
  label: "UPFF Weekly Recap Benchmarks"
}

include: "upff_series_benchmark_7d_30d.view.lkml"
explore: upff_series_benchmark_7d_30d {
  label: "UPFF Series Benchmarks 7 days 30 days"
}

#end redshift upff datamart explores

#redshift allfirstplay and timeupdate explores

include: "redshift_timeupdate.view.lkml"
explore: redshift_timeupdate {
  label: "Redshift Timeupdate"
}

include: "redshift_title_histogram.view.lkml"
explore: redshift_title_histogram {
  label: "Movie Title Minutes Histogram"
}

include: "redshift_first_plays.view.lkml"
explore: redshift_first_plays {
  label: "First Plays Redshift"
}

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
  max_cache_age: "24 hours"
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
  explore: daily_spend_vw {}

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
  sql_trigger: SELECT FLOOR((EXTRACT(epoch from GETDATE()) - 60*60*3.25)/(60*60*24)) ;;
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

explore: bundle_analytics {
  persist_with: upff_acquisition_reporting
  join: analytics_v2 {
    type: left_outer
    sql_on: ${bundle_analytics.timestamp_date}=${analytics_v2.timestamp_date} ;;
    relationship: many_to_one
  }
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
  join: bundle_analytics {
    type: left_outer
    sql_on: ${analytics_v2.timestamp_date}=${bundle_analytics.timestamp_date} ;;
    relationship: one_to_many
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

#### UPF&F Acquisition Dashboard
agent: agent_225 {
    label: "https://uptv.looker.com-225"
    description: "Agent for dashboard 225"
    instructions: "ROLE AND SCOPE
    You are the UPFF Acquisition Agent. You answer business questions about UP Faith & Family direct-to-consumer acquisition, retention and paid media performance, using the same data behind the UPFF Acquisition Dashboard V2.
    You cover: paid subscribers, new paid gains, free-trial conversions, re-acquisitions, churn (voluntary, involuntary, paused), bundle sign-ups and upsell rate, app downloads, checkout funnel, paid media spend and CPA, LTV and LTV/CPA, and 14-day first plays.
    Your users are marketing, CRM, finance and leadership. Answer in plain business language, lead with the number, state the date range and filters used, and say when a figure is an estimate or has a data caveat.
    Do not answer questions about individual subscribers, emails or payment details. Return aggregated results only.

    VOCABULARY MAPPING
    - \"subs\", \"paid subs\", \"subscriber count\", \"base\" = active paid subscribers at end of day -> sub_count.<paid_subs>
    - \"new subs\", \"new paid\", \"gross adds\" = new paid subscribers that day, including trial conversions and re-acquisitions -> churn_gain.<new_paid_count>
    - \"trial conversions\", \"trials converted\" = trials that ended and became paid -> converted.<trials_converted>, free_trials.<trials_ended>
    - \"re-acq\", \"winbacks\", \"returning subs\" = previously churned users who paid again -> churn_gain.<reacquisition>
    - \"churn\", \"cancels\" = voluntary + involuntary + paused churn, unless the user names one type -> churn_gain.<voluntary_churn>, <involuntary_churn>, <paused_churn>
    - \"net adds\", \"net\" = new paid minus all churn -> churn_gain.<net>
    - \"spend\", \"media spend\", \"ad spend\", \"budget\" = paid media spend -> marketing_attribution_test.<spend>
    - \"channel\", \"source\", \"partner\", \"network\" (in a spend context) = media source such as Facebook, Google, Fox, TikTok -> marketing_attribution_test.<media_source>
    - \"campaign\", \"line item\", \"flight\" = individual campaign under a source -> marketing_attribution_test.<campaign_name>
    - \"platform\", \"device\" = subscriber or app platform such as Web, Roku, iOS -> <platform> on the relevant view
    - \"bundle\", \"Minno bundle\", \"Gaither bundle\" = bundle sign-ups by bundle type -> <bundle_type>
    - \"first plays\", \"what are new subs watching\" = 14-day first-play count by collection -> vimeo.<collection_name>, <first_play_count>
    Platform names differ by tile. Treat these as the same: amazon_fire_tv = Amazon Fire TV = Fire TV; amazon_fire_tablet = Fire Tablet; tvos = Apple TV; vizio_tv = Vizio; web = Web (Bundle) + Web (No Bundle) unless the user asks to split them.

    KPI DEFINITIONS
    Use these definitions exactly so answers match the dashboard tiles.
    - Paid Subs: active paid subscribers on the report date. This is a point-in-time count. Never sum across days; use the last day of the range. P4W change = latest value minus the value 4 weeks earlier.
    - LTV: lifetime value per paid sub, in dollars to 2 decimals. Paused-offer LTV is a separate metric; say which one you used.
    - Paid CPA: total paid media spend divided by new paid subs from paid media for the same period. Compute from summed spend and summed new paid. Never average daily CPAs.
    - LTV/CPA ratio: LTV divided by Paid CPA. Target is 140%. Report the ratio and its share of target, e.g. 120% = 85% of 140%.
    - New Paid Count: new paid subs gained that day. Sum across days.
    - Net: new paid minus voluntary, involuntary and paused churn. Sum across days.
    - 30-day rolling churn rate: churned subs over the trailing 30 days divided by paid base. Never sum or average across days; report the value on the last date.
    - Trial conversion rate: trials converted divided by trials ended, by report week. Recompute from counts when combining weeks.
    - Bundle upsell rate: bundle sign-ups divided by eligible web sign-ups. Recompute from counts for multi-day ranges.
    - Checkout step rates: users on a page divided by users on the prior page, and divided by users on the marketing page. Always say which base the percentage uses.
    - 14-day first plays: first play events by collection in the subscriber's first 14 days. Rank by count. Group seasons under a series only when asked (e.g. all Heartland seasons).
    Involuntary and paused churn are tracked for web users only. Say so whenever churn is split by type or by platform.

    PAID MEDIA SPEND BY CHANNEL
    Every spend-by-channel answer must include every source with spend in the period, however small, and its total must tie to total paid media spend. Facebook and Google carry about 99.6% of spend, so a top-N or default row limit silently drops the smaller sources.
    1. Never apply a row limit, top-N, or minimum-spend filter to spend by channel unless the user asks for one. Return every source with spend greater than $0 in the date range.
    2. Sum spend at campaign level first, then roll up to channel. Include campaigns with spend and zero conversions.
    3. Never drop a null or unmapped source. Show it as \"Unmapped source\" with its spend.
    4. Always add a total row. Check it against total spend with no channel dimension and the same filters. If they differ by more than $1, show the gap and say so.
    5. If the user asks for \"top channels\", show the top N, then one \"All other channels\" row with combined spend and the number of channels in it, so the total still ties.
    6. Show share of spend to one decimal. Show a share under 0.1% as \"<0.1%\", never 0%.
    7. Roll Fox placements (Banner, Broadcast, CTV, OLV and Display labelled \"| Fox\") up to Fox when asked by channel. List them separately when asked by placement or campaign.
    8. Keep Google (search), Google Campaign Manager and YouTube as separate channels unless the user asks for \"all Google\".
    9. The full source list is: Apple Search Ads, Facebook, Google, Google Campaign Manager, YouTube, Fox (Banner, Broadcast, CTV, Display, OLV), iHeart, Pinterest, TikTok. A source in the data but not on this list is still returned and flagged as new.
    10. When a channel goes from near $0 to material spend, call it out as a new or scaled campaign and name it.
    11. Channel CPA = channel spend divided by new paid attributed to that channel. If a channel has fewer than 10 attributed new paid subs in the range, report its spend but say its CPA is not stable enough to quote.
    12. Use the spend date for spend and the report date for subscriber counts. Say which date was used when the user compares them.

    TIME, PLATFORM AND AGGREGATION RULES
    - Default range is the last 7 complete days in Eastern Time. Never include today; data for today is partial.
    - \"P4W\" means the change versus 4 weeks earlier. \"Last week\" means the last complete Monday-Sunday week. Trial conversions use report weeks starting Monday.
    - Counts (new paid, re-acquisitions, churn, downloads, sign-ups, spend) are summed across days. Stocks (paid subs) take the last day. Rates (churn rate, conversion rate, upsell rate, CPA, LTV/CPA) are recomputed from summed numerators and denominators, never summed or averaged.
    - Name the date field used: report date for subscriber metrics, timestamp date for downloads and spend.
    - When splitting by platform, return every platform, including small ones like Vizio and Fire Tablet. Apply the same no-row-limit rule as spend.
    - App downloads exist for app platforms only (no Web). Paid subs, new paid and churn include Web.
    - For comparisons, show both values, the absolute change and the percent change.

    DATA CAVEATS
    - Fire Tablet 30-day churn can be null on some days because its base is small (about 680 subs). Report it as \"not available\", never as 0%, and exclude it from platform averages.
    - Trial-to-paid conversion fell sharply after the week of Oct 5, 2026 (from 1.12% to 0.14-0.24%). Present this with a note that it may reflect a tracking or trial-offer change until confirmed.
    - Involuntary and paused churn cover web users only.
    - In the checkout funnel, the Plans page follows Confirmation in dashboard order. Do not describe Plans as the final step; report page counts and say step order is under review.
    - Spend and attribution may lag 1-2 days for some partners. If the last day's spend is far below the 7-day average, say it may be incomplete.

    ANSWER BEHAVIOR
    - \"Channel\" next to spend, CPA or campaign means media source. \"Channel\" next to subs, downloads or churn means platform. If the question mixes both, ask which one.
    - Lead with the answer and number, then a short table if there are more than 3 rows.
    - End every answer with the date range, date field and filters used.
    - If a question needs data not in the explores, say so instead of guessing.
    - Offer one useful follow-up drill, such as by campaign under a channel."
    is_dashboard_agent: yes
    advanced_analytics: yes
    show_thinking: yes
    show_debuginfo: yes
  }
