connection: "upff"

# include views
include: "ios_users.view"
include: "javascript_users.view"
include: "javascript_identifies.view"
include: "android_users.view"
include: "javascript_subscribed.view"
include: "javascript_users.view"
include: "javascript_play.view"
include: "redshift_php_get_mobile_app_installs.view"
include: "ios_authentication.view"
include: "ios_subscribetapped.view"
include: "android_subscribetapped.view"
include: "ios_signup.view"
include: "android_signup.view"
include: "ios_welcomebrowse.view"
include: "android_welcomebrowse.view"
include: "ios_conversion.view"
include: "android_conversion.view"
include: "redshift_php_get_analytics.view"
include: "redshift_android_firstplay.view"
include: "redshift_pixel_api_email_opened.view"
include: "redshift_android_application_installed.view"
include: "redshift_ios_application_installed.view"
include: "redshift_php_get_trialist_survey.view"
include: "redshift_derived_personalize.view"
include: "redshift_php_send_trialist_survey.view"
include: "redshift_php_get_churn_survey.view"
include: "redshift_python_users.view"
include: "javascript_conversion.view"
include: "redshift_get_titles.view"
include: "javascript_conversion.view"
include: "redshift_php_get_user_on_email_list.view"
include: "redshift_marketing_performance.view.lkml"
include: "redshfit_marketing_installs_1.view.lkml"
include: "redshift_javascript_conversion.view.lkml"
include: "redshift_roku_firstplay.view.lkml"
include: "redshift_javascript_upff_home_pages.view.lkml"
include: "redshift_mobile_conversions.view.lkml"
include: "redshift_marketing_performance_v2.view.lkml"
include: "amazon_personalize_recommendations.view.lkml"
include: "redshift_looker_customer_conversion_scores.view.lkml"
include: "redshift_php_get_average_predicted_conversion_score.view.lkml"
include: "redshift_php_get_referral_program_info.view.lkml"
include: "redshift_php_get_email_campaigns.view.lkml"
include: "redshift_derived_mobile_app_engagement.view.lkml"
include: "redshift_derived_added_to_watch_list.view.lkml"
include: "redshift_get_email_automation_emails.view.lkml"
include: "redshift_get_email_automations.view.lkml"
include: "redshift_data_warehouse_info.view.lkml"
include: "redshift_segment_anonymous_known_users.view.lkml"
include: "redshift_looker_get_kpis.view.lkml"
include: "redshift_javascript_mybundle_tv.view.lkml"
include: "redshift_javascript_mybundle_tv_signup.view.lkml"
include: "redshift_php_mybundle_library.view.lkml"
include: "video_content_playing_by_source.view.lkml"
include: "redshift_get_mailchimp_campaigns.view.lkml"
include: "redshift_http_api_zendesk_vimeo_ott_users.view.lkml"
include: "redshift_looker_upff_email_list.view.lkml"
include: "redshift_looker_get_titles.view.lkml"
include: "redshift_custom_cross_platform_logins.view"
include: "redshift_php_get_analytics_real_time.view"
include: "redshift_javascript_search_executed.view"
include: "redshift_active_customers.view"
include: "redshift_customers_resubscribers.view"
include: "redshift_customers_views_by_user.view"
include: "/views/monthly_customer_report.view.lkml"
include: "/views/agm_audiences.view.lkml"
include: "/views/bango_views/verizon_events.view.lkml"
include: "/page_views_ip_date.view.lkml"
include: "/segment_consent.view.lkml"
include: "test_all_customers.view.lkml"
include: "/video_views.view.lkml"
include: "/Gaither_page_views.view.lkml"
include: "/gaither_segment_consent.view.lkml"
include: "/views/up_airtable_reports.view.lkml"
include: "/Vimeo_OTT/vimeo_ott_all_customers.view.lkml"
include: "/Vimeo_OTT/vimeo_ott_all_customers_workflows.view.lkml"
include: "/views/Marketing_attribution/upff_signup_funnel.view.lkml"

explore: upff_signup_funnel {
  label: "UPFF Sign-Up Funnel (iOS, Android, Web)"
  description: "Entry -> Order funnel across iOS, Android and Web. Choose the Current and Prior periods and the attribution window; filter by Platform or Platform Group."

  # Defaults: this week (last 7 full days) vs last week, 3-day attribution window.
  # Business users can change these to any date ranges in the filter bar.
  always_filter: {
    filters: [
      upff_signup_funnel.current_period: "7 days ago for 7 days",
      upff_signup_funnel.prior_period: "14 days ago for 7 days",
      upff_signup_funnel.attribution_days: "3"
    ]
  }
}

agent: agent_264 {
  label: "https://uptv.looker.com-264"
  description: "Agent for dashboard 264"
  instructions: "Instructions

  ROLE
  You are a subscription funnel analyst for UP Faith & Family (UPFF), a faith-adjacent streaming service offering family-friendly, uplifting and inspirational entertainment. Describe UPFF as faith-adjacent, family-friendly or uplifting, never as a faith-based or religious service. Answer using only the UPFF Sign-Up Funnel explore and the BENCHMARKS section below. Lead with the number, then context, then (when useful) what it suggests going forward.

  THE FUNNEL (4 steps + OVERALL)
  Step 1 Entry: App Installed (iOS, Android, Roku, Amazon Fire TV, Vizio TV) or Landing Page Visit (first page view on the marketing site, Web)
  Step 2: Sign Up Viewed (apps) / Product Viewed (web)
  Step 3: Plan Chosen (apps) / Signed Up (web)
  Step 4: Order Completed (a new subscription)
  Step 5: OVERALL: Entry -> Order (summary row)
  There is no \"Checkout Started\" step. Never mention or invent one.

  KEY DEFINITIONS
  - Entries: users who entered the funnel (app installs plus marketing-site visitors).
  - Conversions: users who completed an order within the attribution window. Synonyms: sign-ups, subscriptions, new subscribers, orders, purchases.
  - Conversion Rate: conversions / entries (effective entry-to-order rate). This is THE conversion rate for \"conversion rate\", \"sign-up rate\" or \"overall conversion\".
  - Average Daily Conversion Rate: average of each day's rate, every day weighted equally. Use only when asked for a typical day or daily average.
  - Step Conversion Rate: share of the PREVIOUS step that reached this step. Use for drop-off questions. Blank for step 1 by design.
  - Percent of Entries Reaching Step: share of ALL entries reaching each step (starts at 100%). Use for \"how many visitors make it to X\" and for funnel charts.
  - Rate changes are in percentage points (pp). Say \"up 1.5 points\", not \"up 1.5%\".

  PERIODS
  - Current Period and Prior Period are filters. Defaults: Current = last 7 full days, Prior = the 7 days before. Attribution Window default = 3 days.
  - For other comparisons, set both periods (e.g. this month vs last month, or the same dates last year). When asked what-if questions about the window (\"give people 7 days\"), change Attribution Window (Days).
  - Always state the date ranges used.
  - If the Current Period ends within the attribution window of today, note that current rates may still rise.
  - If periods differ in length, compare rates, not raw counts.

  PLATFORMS
  - Platform = iOS, Android, Roku, Amazon Fire TV, Vizio TV or Web.
  - Platform Group = Mobile App (iOS + Android), Connected TV (Roku + Amazon Fire TV + Vizio TV) or Web.
  - \"App\" or \"mobile\" = Platform Group Mobile App. \"CTV\", \"TV apps\", \"smart TV\" or \"OTT\" = Platform Group Connected TV. \"Website\", \"site\" or \"marketing site\" = Platform Web.
  - For platform comparisons, group by Platform or Platform Group with the headline Conversion Rate fields, or use the Platform Comparison measures (Conversion Rate - iOS, Android, Roku, Amazon Fire TV, Vizio TV, Web, Connected TV).
  - Apps start at install and web starts at a site visit, so the all-platform rate blends different starting points; when showing it, recommend the per-platform view for like-for-like comparison.
  - Web Share of Conversions mixes both periods unless filtered: filter Period to Current (or Prior), and do not filter by Platform.

  WEB MARKETING CHANNELS AND CAMPAIGNS
  - UTM fields exist for Web only and come from each web user's first site visit in the period (first touch).
  - To FILTER by channel or campaign, use the Web Campaign Filters: Marketing Platform Filter, Campaign Source Filter, Campaign Name Filter, Campaign Medium Filter. They narrow Web users only; mobile and Connected TV users always stay in the results. When reporting a filtered result, say that only web was filtered.
  - To COMPARE channels or campaigns, group by Marketing Platform, Campaign Source, Campaign Name, Campaign Medium, Campaign Content or Campaign Term, and filter Platform to Web. These grouping fields cannot be used as filters.
  - Marketing Platform values: Google Search, Google PMax, Google Display, YouTube, Meta Ads, Bing Ads, HubSpot, UPtv Digital, ChatGPT, Organic Search, Organic Social, Others, Unknown. App users show as \"Mobile App (no UTM)\" or \"Connected TV (no UTM)\"; leave those out of channel comparisons.
  - This explore has no spend data, so it cannot calculate CAC, ROAS or cost per subscriber.

  WHICH FIELDS TO USE
  - Headline questions: Conversion Rate (Current / Prior Period), Conversion Rate Change in Percentage Points, Conversions (Current / Prior Period), Conversions Percent Change, Entries (Current / Prior Period).
  - Funnel or drop-off: Funnel Step with Users at Step, Step Conversion Rate, or Percent of Entries Reaching Step (Current / Prior). These require Funnel Step. For charts, filter Funnel Step Number to less than 5.
  - Daily trends: Day of Period with Period pivoted and Conversion Rate.
  - Time to convert: Average Hours from Previous Step with Funnel Step.
  - Channel or campaign performance: Marketing Platform or Campaign Name with Entries, Conversions and Conversion Rate, filtered to Platform Web.

  FORWARD-LOOKING ANALYSIS
  - For trend questions (\"are we improving\", \"where is this heading\"), look beyond two weeks: set Current Period to a longer range (e.g. last 8 or 12 weeks) and group Entries, Conversions and Conversion Rate by Entry Week. Describe direction (rising, flat, declining), size of change per week, and any break in the pattern.
  - Momentum: compare the most recent 2 weeks with the 4-6 weeks before them, and say whether the change is accelerating or slowing.
  - Projections: you may give a simple run-rate estimate (e.g. \"at the current weekly pace, about X conversions in the next 4 weeks\"). Always label it an estimate, state the assumption (current trend continues), give a range rather than a single number when weeks vary, and never present it as a forecast of revenue.
  - Leading indicators: a change in Step 2 or Step 3 rates usually shows up in conversions later; flag these as early signals.
  - Exclude incomplete weeks and the most recent days still inside the attribution window from trend lines and projections, and say so.
  - Recommendations: when the data points to a clear opportunity (e.g. the step with the biggest drop, a platform or channel lagging others), suggest one or two concrete areas to investigate. Frame them as hypotheses, not conclusions.

  COMPETITIVE BENCHMARKING
  - UPFF competes with faith-adjacent, family-friendly and uplifting entertainment streaming services, including faith-based services, family and feel-good streamers, and general streamers' family offerings. When comparing, note how closely each benchmark's audience and content positioning matches UPFF's.
  - Only use benchmark figures listed in the BENCHMARKS section. Quote the figure, its source and date.
  - Never invent, estimate or recall competitor metrics, subscriber counts or conversion rates from general knowledge. If no benchmark exists for the question, say so and answer with UPFF's own trend instead.
  - Compare like for like: match the benchmark definition (e.g. trial-to-paid vs visit-to-subscribe, app vs CTV vs web) before comparing, and say when definitions differ.
  - Describe position relative to the benchmark (above, in line, below) and by how many points, without overstating precision.

  BENCHMARKS
  (Maintained by the analytics team. Add one line per benchmark: metric, value, definition, segment, source, date.)
  - [Example format] Web visit-to-subscribe conversion, X.X%, landing visit to paid order within 3 days, DTC streaming, [source], [date]
  - [Example format] CTV install-to-subscribe conversion, X.X%, install to paid order, family streaming TV apps, [source], [date]

  ANSWER STYLE
  - Percentages to 2 decimals, counts with commas, hours to 1 decimal.
  - When comparing periods, give Current, Prior and the change.
  - For drop-off questions, name the step with the largest decline first.
  - If the question needs data this explore doesn't have (revenue, churn, cancellations, viewing, marketing spend or CAC, competitor data not in BENCHMARKS), say so plainly.

  SUGGESTED QUESTIONS
  Offer these as starting points when a user is unsure what to ask. All can be answered from the UPFF Sign-Up Funnel explore.

  1. What's our conversion rate this week compared to last week?
  2. How many new subscribers did we get this week, and is that up or down from last week?
  3. Where in the funnel are web visitors dropping off the most?
  4. How does iOS conversion compare to Android and web this month vs last month?
  5. What percent of landing page visitors end up subscribing?
  6. Is our conversion rate trending up or down over the last 12 weeks?
  7. Which platform had the biggest change in conversion rate this week vs last week?
  8. Which funnel step changed the most this week compared to last week?
  9. Of the people who viewed the sign-up page or product page, what share went on to choose a plan or sign up?
  10. How long does it take people to subscribe after visiting the site or installing the app, and is that getting faster?
  11. Which day of the week brings in the most new subscribers, and does it convert better?
  12. How does this September compare to September last year?
  13. Is the mobile app or the website bringing in more of our new subscribers, and is that shifting?
  14. Which funnel step takes people the longest to get through on Android?
  15. How does our conversion rate change if we give people 7 days instead of 3 to subscribe?
  16. Show me conversion rate day by day this week compared to last week.
  17. Our conversion rate looks different from the average daily rate. Why?
  18. Are fewer people viewing the sign-up page after installing the app this week compared to last week?
  19. How many people installed the iOS app this week, and how many of them subscribed?
  20. For web, which step had the biggest change in the share of visitors reaching it this week vs last week?"
  is_dashboard_agent: yes
  show_thinking: yes
  show_debuginfo: yes
}


explore: vimeo_ott_all_customers_workflows {
  label: "Vimeo OTT – All Customers Workflows"
  description: "Explore UP Faith & Family All Customer dataset"

  join: redshift_php_get_trialist_survey {
    type: left_outer
    sql_on: ${redshift_php_get_trialist_survey.user_id}=${vimeo_ott_all_customers_workflows.user_id};;
    relationship: one_to_one
  }
}

explore: vimeo_ott_all_customers {
  label: "Vimeo OTT – All Customers"
  description: "Explore UP Faith & Family All Customer dataset"

  join: redshift_php_get_trialist_survey {
    type: left_outer
    sql_on: ${redshift_php_get_trialist_survey.user_id}=${vimeo_ott_all_customers.user_id};;
    relationship: one_to_one
  }
}

explore: up_airtable_reports {
  label: "Linear TV Schedule"
  description: "Explore programming schedule from Mediagenix for UPtv, Ovation, and Gaither"
}

explore: video_views {
  label: "Video Views"
}

explore: test_all_customers {
  label: "Test All Customers"
}

explore: page_views_ip_date{
  label: "Page Views Join by IP"
  join: segment_consent {
    sql_on: ${page_views_ip_date.context_ip} = ${segment_consent.context_ip}
      AND DATE(${page_views_ip_date.timestamp_time}) = DATE(${segment_consent.timestamp_time}) ;;
    relationship: many_to_one
  }
}

explore: gaither_page_views{
  label: "Gaither Page Views Join by IP"
  join: gaither_segment_consent {
    sql_on: ${gaither_page_views.context_ip} = ${gaither_segment_consent.context_ip}
      AND DATE(${gaither_page_views.timestamp_time}) = DATE(${gaither_segment_consent.timestamp_time}) ;;
    relationship: many_to_one
  }
}

explore: page_views{
  label: "Page Views"
  from: page_views_ip_date
}
explore: gaither_segment_consent {
  label: "Gaither Segment Consent"
}
explore: segment_consent {
  label: "Segment Consent"
}

explore: agm_audiences {
  label: "AGM Audiences"
}

explore: verizon_events {
  label: "Verizon +play Events"
}

explore: redshift_customers_views_by_user {
  label: "Views By User"
}

explore: redshift_customers_resubscribers{
  label: "Re-Subscribers"
}

explore: redshift_active_customers {
  label: "Active Customers"
}

include: "redshift_dunning.view"
explore: redshift_dunning{
  label: "Dunning Results"
}

include: "recovery_rates.view"
explore: recovery_rates {
  label: "Recovery Results"
}

include: "recovery_rates_monthly.view"
explore: recovery_rates_monthly {
  label: "Recovery Results Monthly"
}

include: "daily_churn.view"
explore: daily_churn {
  label: "Daily Churn"
}

explore: redshift_javascript_search_executed {
  label: "Web Search Executed"
}

explore:redshift_php_get_analytics_real_time{
  label: "Real-Time Analytics"
}


explore: redshift_custom_cross_platform_logins  {
  label: "Logins"
}

explore: redshift_looker_get_titles {
  label: "Get Titles"
}

explore: redshift_http_api_zendesk_vimeo_ott_users {
  label: "Zendesk Vimeo OTT Users"

  join: redshift_php_get_trialist_survey {
    type: left_outer
    sql_on: ${redshift_php_get_trialist_survey.user_id}=${redshift_http_api_zendesk_vimeo_ott_users.user_id};;
    relationship: many_to_many
  }
}

explore: video_content_playing_by_source {
  label: "Video Content Playing"

}

explore: redshift_php_mybundle_library {
  label: "My Bundle.TV Library Feed"

}

explore: redshift_javascript_mybundle_tv_signup {
  label: "My Bundle.TV Signup"

}

explore: redshift_javascript_mybundle_tv {
  label: "My Bundle.TV Free Trial & Paid"
}

explore: redshift_looker_get_kpis {
  label: "Get KPIs"
}

explore: redshift_segment_anonymous_known_users {
  label: "Segment Monthly Tracked Users"
}

explore: redshift_data_warehouse_info {
  label: "Redshift DW Info"
}

explore: redshift_get_email_automation_emails {
  label: "Email Automation Emails"
}

explore: redshift_get_email_automations {
  label: "Email Automations"
}

explore: redshift_derived_added_to_watch_list{
  label: "Add Watch List"
}

explore: redshift_derived_mobile_app_engagement {
  label: "Mobile App Engagement"
}

explore: redshift_php_get_email_campaigns {
  label: "Email Campaigns"
}

explore: redshift_get_mailchimp_campaigns {
  join: http_api_purchase_event {
    type: inner
    sql_on: ${http_api_purchase_event.email}=${redshift_get_mailchimp_campaigns.email} ;;
    relationship: many_to_many
  }
}


explore: redshift_looker_customer_conversion_scores {
  join: redshift_php_get_average_predicted_conversion_score {
    type: left_outer
    sql_on:  ${redshift_looker_customer_conversion_scores.received_date} = ${redshift_php_get_average_predicted_conversion_score.received_date_date};;
    relationship: many_to_one
  }
}

explore: amazon_personalize_recommendations {}

explore: redshift_marketing_performance_v2 {}

explore: redshift_roku_firstplay {

}

explore: redshift_marketing_performance {
  join: redshift_javascript_conversion {
    type: left_outer
    sql_on: ${redshift_javascript_conversion.ad_id}=${redshift_marketing_performance.ad_id} and ${redshift_javascript_conversion.timestamp_date}=${redshift_marketing_performance.timestamp_date};;
    relationship: many_to_one
  }
  join: redshfit_marketing_installs_1 {
    type: left_outer
    sql_on: ${redshfit_marketing_installs_1.ad_id}=${redshift_marketing_performance.ad_id} and ${redshift_marketing_performance.timestamp_date}=${redshfit_marketing_installs_1.timestamp_date};;
    relationship: many_to_one
  }
  join: redshift_mobile_conversions {
    type: left_outer
    sql_on: upper(${redshfit_marketing_installs_1.anonymous_id})=upper(${redshift_mobile_conversions.anonymous_id});;
    relationship: many_to_one
  }
}


explore: ios_users {
  label: "Web and iOS App Users"
  join: javascript_users {
    type:  left_outer
    sql_on: ${javascript_users.id} = ${ios_users.id} ;;
    relationship: one_to_one
  }

  join: javascript_identifies {
    type:  inner
    sql_on: ${ios_users.id} = ${javascript_identifies.user_id} ;;
    relationship: one_to_one
  }

}

explore: android_users {
  label: "Web and Android App Users"
  join: javascript_users {
    type:  inner
    sql_on: ${javascript_users.id} = ${android_users.id} ;;
    relationship: one_to_one
  }

  join: javascript_identifies {
    type:  inner
    sql_on: ${android_users.id} = ${javascript_identifies.user_id} ;;
    relationship: one_to_one
  }

}

explore: web_to_ios{
  label: "Web to iOS Subscribers"
  from: subscribed

  join: javascript_users {
    sql_on: ${javascript_users.id} = ${web_to_ios.user_id};;
    relationship: one_to_one
  }


  join: ios_users {
    type: inner
    sql_on: ${javascript_users.id} = ${ios_users.id} ;;
    required_joins: [javascript_users]
    relationship: one_to_one
  }
}

explore: web_to_android{
  label: "Web to Android Subscribers"
  from: subscribed

  join: javascript_users {
    sql_on: ${javascript_users.id} = ${web_to_android.user_id};;
    relationship: one_to_one
  }

  join: android_users {
    type: inner
    sql_on: ${javascript_users.id} = ${android_users.id} ;;
    relationship: one_to_one
  }

}

# Web Suscribers
explore: javascript_subscribed {

  label: "Web Subscribers"
  from: subscribed

  join: javascript_users {
    type:  inner
    sql_on: ${javascript_subscribed.user_id} = ${javascript_users.id} ;;
    relationship: one_to_one
  }

}

# Web Suscriber Plays
include: "javascript_firstplay.view"
explore: javascript_users {

  label: "Web Subscriber Plays"

  join: javascript_play {
    type:  inner
    sql_on: ${javascript_users.id} = ${javascript_play.user_id} ;;
    relationship: one_to_one
  }


  join: javascript_firstplay {
    type:  inner
    sql_on: ${javascript_users.id} = ${javascript_firstplay.user_id} ;;
    relationship: one_to_one
  }

  join: javascript_conversion {
    type:  inner
    sql_on: ${javascript_users.id} = ${javascript_conversion.user_id};;
    relationship: one_to_one
  }

}

include: "javascript_uptv_pages.view"
explore: javascript_uptv_pages {
  label: "Cross-Domain Subs"
  join: subscribed {
    type:  left_outer
    sql_on: ${javascript_uptv_pages.context_traits_cross_domain_id} = ${subscribed.context_traits_cross_domain_id} ;;
    relationship: one_to_one
  }

  join: javascript_users {
    type:  left_outer
    sql_on: ${javascript_uptv_pages.context_traits_cross_domain_id} = ${javascript_users.context_traits_cross_domain_id} ;;
    relationship: one_to_one
  }

  join: javascript_play {
    type: left_outer
    sql_on: ${javascript_uptv_pages.context_traits_cross_domain_id} = ${javascript_play.context_traits_cross_domain_id};;
    relationship: one_to_one
  }
}




include: "php_get_customers.view"
explore: php_get_customers{
  label: "Mktg Opt-In Subscribers"
  description: "Marketing Opt-In Subs"
}

include: "delighted_survey_question_answered.view"
include: "mailchimp_email_campaigns.view"


explore: mailchimp_email_campaigns {}

include: "javascript_subscribed.view"
include: "http_api_purchase_event.view"
explore: subscribed {}
explore: http_api_purchase_event
{
  label: "Subscribers"

  join: redshift_php_get_referral_program_info {
    type: inner
    sql_on: ${http_api_purchase_event.user_id} = ${redshift_php_get_referral_program_info.user_id};;
    relationship: one_to_one
  }


  join: redshift_php_send_trialist_survey {
    type: left_outer
    sql_on: ${http_api_purchase_event.user_id} = ${redshift_php_send_trialist_survey.user_id};;
    relationship: one_to_one
  }

  join: redshift_php_get_trialist_survey{
    type: left_outer
    sql_on: ${http_api_purchase_event.user_id} = ${redshift_php_get_trialist_survey.user_id};;
    relationship: one_to_one
  }

  join: redshift_php_get_churn_survey {
    type: left_outer
    sql_on: ${http_api_purchase_event.user_id} = ${redshift_php_get_churn_survey.user_id};;
    relationship: one_to_one
  }

  join: redshift_pixel_api_email_opened{
    type: left_outer
    sql_on: ${http_api_purchase_event.user_id} = ${redshift_pixel_api_email_opened.user_id};;
    relationship: many_to_many
  }

  join: redshift_get_mailchimp_campaigns{
    type: left_outer
    sql_on: ${http_api_purchase_event.email}=${redshift_get_mailchimp_campaigns.email} ;;
    relationship: one_to_one
  }

  join: redshift_php_get_email_campaigns{
    type:  left_outer
    sql_on:  ${redshift_php_get_email_campaigns.timestamp_date} = ${redshift_get_mailchimp_campaigns.timestamp_date} ;;
    relationship: many_to_many
  }

  join: redshift_looker_upff_email_list{
    type:  left_outer
    sql_on:  ${redshift_looker_upff_email_list.campaigns_timestamp_date_date} = ${redshift_get_mailchimp_campaigns.timestamp_date} ;;
    relationship: many_to_many
  }

  join: delighted_survey_question_answered {
    type: left_outer
    view_label: "Delighted: No Surveyed"
    sql_on: ${delighted_survey_question_answered.user_id} != ${http_api_purchase_event.user_id};;
    relationship: one_to_one
  }

  join: android_users {
    type: left_outer
    sql_on: ${http_api_purchase_event.user_id} = ${android_users.id};;
    relationship: one_to_one
  }

  join: android_conversion {
    type: left_outer
    sql_on: ${android_users.context_traits_anonymous_id} = ${android_conversion.anonymous_id};;
    relationship: one_to_one
  }

  join: redshift_php_get_mobile_app_installs {
    type: left_outer
    sql_on: ${redshift_php_get_mobile_app_installs.anonymous_id} = ${android_conversion.anonymous_id};;
    relationship: one_to_one
  }

  join: redshift_php_get_user_on_email_list {
    type: left_outer
    sql_on: ${http_api_purchase_event.email} = ${redshift_php_get_user_on_email_list.email};;
    relationship: one_to_one
  }
}


#Delighted.com // Feedback Survey Responses
explore: delighted_survey_question_answered {
  label: "Delighted Feedback"

  join: http_api_purchase_event {
    type: left_outer
    sql_on: ${delighted_survey_question_answered.user_id} = ${http_api_purchase_event.user_id};;
    relationship: one_to_one
  }

  join: redshift_pixel_api_email_opened {
    type: left_outer
    sql_on: ${delighted_survey_question_answered.user_id} = ${redshift_pixel_api_email_opened.user_id};;
    relationship: one_to_one
  }

  join: mailchimp_email_campaigns {
    type:  inner
    sql_on: ${mailchimp_email_campaigns.campaign_date} = ${delighted_survey_question_answered.timestamp_date};;
    relationship: one_to_one
  }


}

include: "ios_firstplay.view"
#iOS // get user plays
explore: ios_users_firstplay {
  label: "iOS Subscribers Play"
  from:  ios_users

  join: ios_firstplay {
    type: inner
    sql_on: ${ios_users_firstplay.id} = ${ios_firstplay.user_id};;
    relationship: one_to_one
  }
}

include: "android_users.view"
#Android // get user plays
explore: android_users_play {
  label: "Android Subscribers Play"
  from:  android_users
}

include: "javascript_pages.view"
include: "ios_view.view"
include: "android_view.view"
include: "android_signin.view"
include: "ios_signin.view"
include: "signupstarted.view"
include: "customers_social_ads.view"
include: "ios_signupstarted.view"

explore: javascript_pages {label: "Web Pages Views"}
explore: ios_view {label: "iOS Views"}
explore: android_view {label: "Android Views"}
explore: android_signin {label: "Android Sign-in"}
explore: ios_signin { label: "iOS Sign-in"}
explore: android_signupstarted {
  label: "Android Signupstarted"

  join: customers_social_ads {
    type: inner
    sql_on: ${android_signupstarted.context_device_advertising_id} = ${customers_social_ads.user_data_aaid};;
    relationship: one_to_one
  }

  join: android_users {
    type: inner
    sql_on: ${android_signupstarted.context_traits_user_id} = ${android_users.id};;
    relationship: one_to_one
  }
}
explore: ios_signupstarted {
  label: "iOS Signupstarted"

  join: customers_social_ads {
    type: inner
    sql_on: ${ios_signupstarted.context_device_advertising_id} = ${customers_social_ads.user_data_idfa};;
    relationship: one_to_one
  }

  join: ios_users {
    type: inner
    sql_on: ${ios_signupstarted.user_id} = ${ios_users.id};;
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

explore: javascript_timeupdate {label: "Web Timeupdate"}
explore: ios_timeupdate {label: "iOS Timeupdate"}
explore: android_timeupdate {}
explore: javascript_authentication {label: "Web Authentication"}
explore: javascript_derived_timeupdate {}
explore: derived_marketing_attribution {label: "Attribution: Cross Platform"}
explore: ios_branch_install {label: "iOS Branch Install"}
explore: ios_branch_open {label: "iOS Branch Open"}
explore: ios_branch_reinstall {label: "iOS Branch Re-Install"}
explore: ios_identifies {label: "iOS Identifies"}
explore: android_branch_install {label: "Android Branch Install"}
explore: android_branch_reinstall {label: "Android Branch Re-Install"}
explore: derived_subscriber_platform_total {label: "Subscriber Platform Total"}
explore: customers_social_ads {

  label: "Marketing Attribution"
  join: ios_signupstarted {
    type: inner
    sql_on: ${customers_social_ads.user_data_idfa} = ${ios_signupstarted.context_device_advertising_id};;
    relationship: one_to_one
  }
}

explore: redshift_php_get_mobile_app_installs {

  label: "Mobile Attribution"
  join: ios_conversion {
    type: left_outer
    sql_on: ${redshift_php_get_mobile_app_installs.anonymous_id} = ${ios_conversion.anonymous_id};;
    relationship: one_to_one
  }

  join: android_conversion {
    type: left_outer
    sql_on: ${redshift_php_get_mobile_app_installs.anonymous_id} = ${android_conversion.anonymous_id};;
    relationship: one_to_one
  }

  join: android_users {
    type: inner
    sql_on: ${android_conversion.anonymous_id} = ${android_users.context_traits_anonymous_id};;
    relationship: one_to_one
  }

  join: ios_users {
    type: inner
    sql_on: ${ios_conversion.context_ip} = ${ios_users.context_ip};;
    relationship: one_to_one
  }

  join: ios_signin {
    type: inner
    sql_on: ${ios_conversion.anonymous_id} = ${ios_signin.anonymous_id};;
    relationship: one_to_one
  }

  join: authentication {
    type: inner
    sql_on: ${ios_conversion.anonymous_id} = ${authentication.anonymous_id};;
    relationship: one_to_one
  }

}

explore: redshift_python_users {

  join: http_api_purchase_event {
    type: left_outer
    sql_on:  ${http_api_purchase_event.user_id} = ${redshift_python_users.id} ;;
    relationship: one_to_one
  }

  join: redshift_get_titles {
    type: left_outer
    sql_on:  ${redshift_python_users.recommended_title_one} = ${redshift_get_titles.video_id};;
    relationship: one_to_one
  }

}


explore: javascript_conversion {}

explore: redshift_android_firstplay {}
explore: redshift_derived_personalize {
  label: "Amazon Personalize Dataset"
}

explore: monthly_customer_report {
  label: "Monthly Customer Report"
}
