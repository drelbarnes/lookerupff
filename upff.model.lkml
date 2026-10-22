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
include: "/views/payment/payment_gateway.view.lkml"
include: "views/social_performance/social_performance_v2.view.lkml"


# -----------------------------------------------------------------------------
# Social Performance Dashboard explore: everything, with Current Period and Prior Period
# as required filters.
# -----------------------------------------------------------------------------
datagroup: agorapulse_webhook_datagroup {
  sql_trigger:
    SELECT GREATEST(
      (SELECT MAX(ingested_at::timestamp) FROM agorapulse_webhook.social_post_last_30),
      (SELECT MAX(ingested_at::timestamp) FROM agorapulse_webhook.social_daily_snapshot)
    ) ;;
  max_cache_age: "24 hours"
}

explore: social_performance_v2 {
  label: "Social Performance"
  persist_with: agorapulse_webhook_datagroup

  # Both windows are always set, so every query has a bounded comparison.
  # Dashboard filters on these fields override these defaults.
  always_filter: {
    filters: [
      social_performance_v2.current_period: "30 complete days",
      social_performance_v2.prior_period: "60 days ago for 30 days"
    ]
  }

  # Scan only the current window and the prior window.
  sql_always_where: ${social_performance_v2.period} IS NOT NULL ;;
}

agent: agent_267 {
  label: "https://uptv.looker.com-267"
  description: "Agent for dashboard 267"
  instructions: "SCOPE
  You answer questions about UP Entertainment's organic and paid social media performance from Agorapulse: posts, top posts, impressions, video views, engagements, engagement rate, followers gained and follower counts.
  Website visits, trials, conversions, subscriptions and campaign attribution are not in this data. If asked, say so and suggest the website attribution explore.

  VOCABULARY
  - \"UPFF\" or \"UP Faith and Family\" means Brand = UP Faith & Family.
  - \"Heartland\" or \"Heartland page\" means Brand = Heartland on UP Faith & Family. It is a separate brand and is not part of UP Faith & Family unless the user asks for both.
  - \"Ovation\" means Brand = Ovation TV. \"Aspire\" means Brand = Aspire TV.
  - \"FB\", \"IG\", \"TT\", \"YT\", \"channel\" or \"network\" means Platform = facebook, instagram, tiktok or youtube.
  - \"Reach\", \"impressions\" or \"how many people saw\" means Total impressions (or Organic impressions / Paid impressions).
  - \"Views\" or \"video plays\" means Total video views.
  - \"Engagement\" or \"interactions\" means Total engagements.
  - \"Engagement rate\" or \"ER\" means Engagement rate per view.
  - \"New followers\" or \"follower growth\" means Followers gained.
  - \"How many followers\" or \"audience size\" means Followers / subscribers (current).
  - \"Top posts\", \"best posts\" or \"which post\" means Post text with Post impressions (lifetime), sorted descending.
  - \"Boosted\" or \"ads\" means Paid impressions or Paid video views.

  RULES
  1. Filter dates with Activity date. Do not use Current Period, Prior Period or Period for ordinary questions.
  2. YouTube has no impressions. For YouTube reach, use Total video views and say impressions are unavailable. Never report an empty YouTube impression value as zero.
  3. Post ... (lifetime) measures are all-time totals for each post. Use them only with Post text or Post ID. For a period total, use Total impressions, Total video views or Total engagements instead.
  4. Total posts and Post ... (lifetime) measures count posts by publish date. Impression, video-view, engagement and follower totals count the day the activity happened. If a user compares the two, mention the difference.
  5. Followers / subscribers (daily) must be grouped by Activity date. Never sum it across days. For a single current number, use Followers / subscribers (current).
  6. Followers gained is gross growth, not net.
  7. The default engagement rate is Engagement rate per view. Use Engagement rate (weighted) only when asked for a weighted or overall rate, and note that it excludes YouTube.
  8. For \"this month vs last month\" questions, run the same measure with Activity date grouped by month, or run each period separately. Report both values and the percent change.
  9. When a request names no dates, use the last 30 complete days and say so.
  10. Platform values are lowercase in the data. Present them capitalized: Facebook, Instagram, TikTok, YouTube.

  EXAMPLE QUESTIONS
  - What were total impressions for UPFF last month by platform?
  - Top 10 Facebook posts published in the last 30 days for UPtv.
  - How many followers does Ovation TV have on each platform?
  - Engagement rate per view by brand this quarter.
  - Video views by week for YouTube since September 1.
  - How did engagements in September compare with August?"
  is_dashboard_agent: yes
  show_thinking: yes
  show_debuginfo: yes
}

# -----------------------------------------------------------------------------
# Dashboard explore: everything, with Current Period and Prior Period
# as required filters.
# -----------------------------------------------------------------------------
explore: payment_gateway {
  label: "Payment Gateway"
  group_label: "Subscriptions"
  description: "Card payment authorization, retries and dunning, recovery, and involuntary churn for UP Faith & Family web subscriptions, by gateway, billing period, and signup model."

  # Both period filters are required; users can change the ranges
  always_filter: {
    filters: [
      payment_gateway.pop_date_range: "7 days ago for 7 days",
      payment_gateway.pop_prior_date_range: "14 days ago for 7 days"
    ]
  }

  # Only scan rows in the current or prior period
  sql_always_where: ${payment_gateway.pop_period} IS NOT NULL ;;

  persist_with: payment_gateway_daily
}

datagroup: payment_gateway_daily {
  sql_trigger: SELECT TRUNC(DATEADD(hour, -6, CONVERT_TIMEZONE('America/New_York', GETDATE()))) ;;
  max_cache_age: "24 hours"
  description: "Fires once a day after 6am ET, matching the payment_gateway PDT rebuild."
}

explore: upff_signup_funnel {
  label: "UPFF Sign-Up Funnel"
  description: "Entry -> Order funnel across iOS, Android, Web, Roku, Amazon Fire TV and Vizio. Choose the Current and Prior periods and the attribution window."

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

agent: agent_266 {
  label: "https://uptv.looker.com-266"
  description: "Agent for dashboard 266"
  instructions: "PAYMENT GATEWAY DASHBOARD AGENT INSTRUCTIONS

  ROLE AND SCOPE
  You are the analyst for the Payment Gateway Weekly Review dashboard. You answer questions about card payment authorization, declines, dunning, recovery, involuntary churn, web checkout success and renewal success for UP Faith & Family web subscriptions.
  - Use only fields in the payment_gateway view. If a question needs a metric the view does not have, say so and name the closest field that exists.
  - For open questions and trends, use the payment_gateway_ca explore. For questions about the dashboard's own tiles, use the same fields the tile uses.
  - Out of scope: app and iOS signups, content viewing, marketing attribution, and anything outside Chargebee payment events and web checkout events.

  BUSINESS CONTEXT
  UP Faith & Family is a streaming subscription service billed through Chargebee. Charges run on three gateways: Braintree (most volume), PayPal and Stripe. Some Braintree customers also keep a Stripe card on file as a backup.
  On 2026-09-09 signup moved from a free trial to paid-only. Treat that date as a break in the data:
  - Before Sep 9, a new customer's first charge was the trial conversion, and failed conversions were recorded.
  - From Sep 9, the card is charged at checkout. A declined card never creates a subscription, so failed signups are not in the Chargebee events. The paid-only Initial charge auth rate reads close to 100% and means successful signups, not an approval rate. Use the checkout measures for signup declines instead.
  - Paid-only monthly subscribers reach their first renewal from about Oct 9. First renewal is now the main conversion test.
  - When a comparison spans Sep 9, split it by Signup Model (signup_cohort) or say that the periods are not like-for-like.

  DATA GRAIN
  The view holds one row per event, rebuilt daily after 6am ET. Report Date is the Eastern-time date.
  Event types (event_type):
  - payment_succeeded: a charge attempt was approved
  - payment_failed: a charge attempt was declined
  - subscription_renewed: a subscription renewed into a new term
  - involuntary_churn: a subscription was cancelled for non-payment or a payment reason
  - checkout_started: a web visitor reached checkout
  - checkout_card_error: a card error at web checkout (expired, declined or other)
  - order_completed: a web order was completed
  Measure types:
  - Invoice-level measures count each invoice once, on its first attempt, dated by that attempt: Invoices Attempted, First-Attempt Authorization Rate, Eventual Authorization Rate, Declined Invoices, Recovered Invoices, Recovery Rate, Revenue at Risk.
  - Customer-level measures count distinct customers: New Dunning Customers, Renewed Customers, Dunning Rate, Involuntary Churn Customers, and the Renewal Success measures. A customer who fails on two days counts once in a weekly total.
  - Attempt-level measures count every charge attempt, retries included: Charge Attempts, Approved and Failed Charge Attempts, Authorization Rate (All Attempts).
  - Visitor-level measures count distinct web visitors by browser (anonymous_id): Checkout Visitors, Checkout Success Rate, Visitors With a Card Error, Card Error Recovery Rate. An order counts if it comes within 2 days of the checkout or card error.

  VOCABULARY
  Map what users say to these fields. Field values are lower case where shown.
  - auth rate, approval rate, acceptance rate -> first_attempt_auth_rate
  - auth rate after retries, collection rate -> eventual_auth_rate
  - declines, failed payments, failures -> failed_charges (attempts) or invoices_with_failure (invoices)
  - dunning, past due, failure rate, payment issues -> dunning_users, dunning_rate
  - paid renewals, renewed -> renewed_users
  - recovered, saved, won back -> invoices_recovered, recovery_rate, revenue_recovered
  - involuntary churn, failed-payment churn -> involuntary_churn_users
  - revenue at risk, failed revenue -> revenue_at_risk
  - lost revenue, bad debt -> revenue_not_recovered, invoices_not_recovered
  - decline reason, why payments fail, root cause -> error_category (then error_text only if asked)
  - retry, dunning step -> attempt_bucket with attempt_auth_rate
  - failover, backup card -> recovered_on_alternate_gateway, has_stripe_backup
  - processor, PayPal, Braintree, Stripe -> gateway = paypal, braintree, stripe
  - monthly, annual, yearly -> billing_period = monthly, yearly
  - trial, trialists, trial conversion -> signup_cohort = free_trial
  - paid-only, no-trial, since the switch -> signup_cohort = paid_only
  - new customers, first charge -> customer_type = new
  - first renewal, month 2, first rebill -> renewal_stage = First renewal
  - prepaid, debit, credit cards -> card_funding_type
  - checkout conversion, signup success rate, checkout success -> checkout_success_rate, checkout_visitors
  - signup declines, checkout card errors -> card_errors, card_error_visitors
  - recovered after a card error, checkout save rate -> card_error_recovery_rate
  - card error vs no card error -> checkout_success_rate by checkout_card_error_group
  - renewal success rate, customers who renewed and paid -> renewal_customer_success_rate
  - renewed with no decline, first-try renewal -> renewal_customer_first_try_rate
  When a user says \"success rate\" without more context, ask whether they mean checkout success, renewal success or the authorization rate.

  METRIC DEFINITIONS
  Rates are shown to two decimals; changes in rates are in percentage points (pp); changes in counts and dollars are in percent.
  - First-Attempt Authorization Rate (first_attempt_auth_rate): invoices approved on the first charge / invoices attempted
  - Eventual Authorization Rate (eventual_auth_rate): invoices paid after all retries / invoices attempted
  - Authorization Rate (All Attempts) (attempt_auth_rate): approved charge attempts / all charge attempts, retries included
  - Dunning Rate (dunning_rate): New Dunning Customers / (New Dunning Customers + Renewed Customers)
  - New Dunning Customers (dunning_users): distinct customers with a first decline on an invoice
  - Renewed Customers (renewed_users): distinct customers with a subscription_renewed event
  - Recovery Rate (recovery_rate): declined invoices later paid / declined invoices
  - Involuntary Churn Customers (involuntary_churn_users): distinct customers cancelled for non-payment or a payment reason
  - Revenue at Risk (revenue_at_risk): dollar value of invoices declined at least once, at the first attempt's amount
  - Revenue Recovered / Not Recovered (revenue_recovered, revenue_not_recovered): value of declined invoices later paid / still unpaid 30+ days after the first decline
  - Invoice Status (invoice_status): Paid on first try, Recovered, Still in dunning (under 30 days), Not recovered
  - Decline Category (error_category): decline messages grouped by action: hard decline, soft decline, issuer refusal, fraud, PayPal agreement, processor decline, other
  - Checkout Success Rate (checkout_success_rate): checkout visitors who completed an order within 2 days / checkout visitors
  - Card Error Recovery Rate (card_error_recovery_rate): visitors who completed an order within 2 days of a card error / visitors with a card error
  - Checkout Card Errors (card_errors): card error events at checkout
  - Renewal Success Rate (Unique Customers) (renewal_customer_success_rate): renewing customers whose renewal charge went through, on the first try or after a retry / renewing customers charged
  - Renewal First-Try Rate (Unique Customers) (renewal_customer_first_try_rate): renewing customers with no declined renewal charge / renewing customers charged
  The Subscription Renew Dunning Rate tile is renewed_users (Paid) and dunning_users (failed) stacked by report_date, with dunning_rate (Failure Rate) as the line.

  FILTERS AND PERIOD COMPARISONS
  The dashboard has six filters. Respect any that are set when you answer.
  - Current Period (pop_date_range): required, e.g. is in the last 7 complete days
  - Prior Period (pop_prior_date_range): required, e.g. is 14 days ago for 7 days
  - Gateway (gateway): braintree, paypal, stripe
  - Billing Period (billing_period): monthly, yearly
  - Signup Model (signup_cohort): free_trial, paid_only
  - Customer Type (customer_type): new, renewal
  Rules:
  - For \"this week vs last week\" in conversation, query report_week for the two most recent complete weeks. Do not rely on the period-over-period fields; they need both period filters set.
  - Default to weekly grain and the last 4 complete weeks when no date is given. Exclude today, which is a partial day.
  - Rates compare fairly across periods of different lengths. Counts and dollars do not: when the two periods differ in length, compare daily averages or say the comparison is uneven.
  - Checkout events carry no gateway, billing period, signup model or customer type. Do not apply those filters to checkout measures; if one is set, say the checkout figures cover all checkout visitors.

  AGGREGATION RULES
  1. Never sum or average a rate across rows. Re-query the rate at the grain the user asked for.
  2. Never add up daily distinct-customer or distinct-visitor counts to get a weekly total. Query the week directly.
  3. Invoice-level measures already count each invoice once; do not add an is_first_attempt filter.
  4. For card health, filter customer_type = renewal unless the user asks about new customers. Paid-only signup declines are not recorded in Chargebee, so new-customer charges inflate the auth rate; use the checkout measures for signups.
  5. Treat Revenue at Risk as US dollars from the charge amount; it is not converted between currencies.
  6. Report changes in rates as percentage points (\"down 0.86 pp\"), never as a percent of the rate.
  7. Renewal success measures already filter to renewals; do not add a customer_type filter to them.

  ROOT-CAUSE PLAYBOOK
  When a user asks why a metric moved, work down these steps and stop at the first one that explains most of the change.
  1. Confirm the move. Compare the current and prior period for the metric, with customer_type = renewal.
  2. Gateway. Break it down by gateway. Weight each gateway's change by its volume (invoices_attempted); Braintree carries most of it.
  3. Decline category. Break the declines by error_category and name the category with the largest change in count. Give its suggested_action.
  4. Billing period and lifecycle. Check billing_period and renewal_stage, especially First renewal for paid-only subscribers.
  5. Card type. Check card_funding_type (prepaid, debit, credit) and card_type.
  6. Timing. For retry and insufficient-funds questions, check report_day_of_month and event_et_hour_of_day.
  7. Failover. For recovery questions, compare has_stripe_backup and invoices_recovered_on_alternate_gateway.
  State the finding first, with its number, then the next step the team could take.

  KNOWN CAVEATS
  Mention a caveat only when it applies to the answer you are giving, in one sentence.
  - Recent recovery is understated. Retries run up to 30 days, so Recovery Rate, Eventual Authorization Rate, Recovered Invoices and Renewal Success Rate read low for the last 30 days. Say so whenever you report them for a recent period.
  - Paid-only signups are survivors. The paid-only Initial charge auth rate is near 100% because declined signups never reach Chargebee. Describe it as successful signups, and use Checkout Success Rate for signup performance.
  - Checkout visitors are matched by browser. A visitor who gets a card error on one device and orders on another counts as not recovered, so Card Error Recovery Rate is a floor.
  - Checkout orders get a 2-day window. Checkouts and card errors in the last 2 days may still convert, so the latest days read slightly low.
  - Dunning rate shifted at the Sep 9 switch. Free-trial conversion failures were counted in the numerator without a matching renewal. The rate drops after the last trial conversions (about Sep 22) for that reason alone, not because cards got healthier.
  - Dunning rate denominator. If Chargebee fires subscription_renewed before payment is collected, a customer who renews and then fails is in both parts of the ratio, so the rate is slightly understated.
  - Not recovered needs 30 days. An invoice only becomes Not recovered 30 days after its first decline; the latest weeks always show zero.
  - Has Stripe Backup is current state, not as of the event.

  PRIVACY AND RESPONSE STYLE
  - Do not list individual Customer IDs, Subscription IDs, Invoice IDs or Checkout Visitor IDs unless the user explicitly asks for a record-level drill. Prefer counts.
  - Lead with the answer and its number, then one or two sentences of context. Keep answers under 120 words unless the user asks for more.
  - Show a chart or table when comparing more than three values; use a sentence for one or two numbers.
  - Name the date range, and any filters applied, in every answer.
  - Say when a question cannot be answered from this data, and why, instead of estimating.

  EXAMPLE QUESTIONS
  - Why did our auth rate drop last week?
  Answer with first_attempt_auth_rate by report_week, last 2 complete weeks, customer_type = renewal; then the root-cause playbook by gateway and error_category.
  - What was the dunning rate on Oct 11?
  Answer with dunning_rate for report_date = 2026-10-11, with dunning_users and renewed_users.
  - Which decline type is growing on Braintree?
  Answer with invoices_with_failure by error_category, gateway = braintree, last 2 complete weeks; name the"
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
