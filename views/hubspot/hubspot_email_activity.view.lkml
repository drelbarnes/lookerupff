###############################################################################
# HubSpot Email Activity  (view 1 of 3)
# -----------------------------------------------------------------------------
# One row per email marketing event: emails sent, delivered, opened, clicked;
# subscribes, unsubscribes, spam reports; form submissions credited to email;
# new contacts. All on one US Eastern timeline, with period-over-period built in.
#
# Built for speed on Redshift:
#   * Campaign name, subject and type are copied into this table, so most
#     dashboard tiles run with no joins at all.
#   * Every metric is a precomputed 0/1 flag, so measures are plain SUMs.
#     "Unique" opens/clicks/deliveries are flagged on each person's first
#     event per campaign, so there is no COUNT(DISTINCT) at query time.
#   * Compound sort key on activity_at: the Reporting Period filter compares
#     the sort column directly, so Redshift skips blocks outside the dates.
#   * Distributed on recipient_email to match the Contacts table, so that
#     join happens without moving data between nodes.
#   * Build: last 13 full months of events, rebuilt once a day (see the
#     datagroup in the model). See BUILD-SPEED NOTES in the SQL below.
#
# How period-over-period works (for developers and the AI agents):
#   * Reporting Period = the date range the user picks.
#   * Compare To = previous period, month, quarter or year.
#   * Plain measures always mean the Reporting Period (all time if unset).
#   * "(Prior Period)" measures mean the comparison window.
#   * "Change %" (counts) and "Change (pts)" (rates) compare the two.
###############################################################################

view: hubspot_email_activity {
  label: "Email Activity"

  derived_table: {
    datagroup_trigger: hubspot_segment_sync
    distribution: "recipient_email"
    sortkeys: ["activity_at", "activity_type"]
    sql:
      -- BUILD-SPEED NOTES
      --  * Rolling window: event data from the last 13 full months (enough for
      --    year-over-year). To change it, edit the -13 / -14 in this SQL.
      --  * received_at filters are redundant with created/timestamp but match
      --    Segment's sort order, so Redshift skips old blocks before reading them.
      --  * Only one full-table window sort remains (the unique-event flag).
      --  * Touch numbers are ranked on a small one-row-per-person-per-campaign
      --    table, then joined back, instead of sorting every event by person.

      SELECT
      a.activity_id,
      a.activity_source,
      a.activity_type,
      a.activity_at,
      a.email_campaign_id,
      a.recipient_email,
      COALESCE(ec.name, CASE WHEN a.email_campaign_id IS NOT NULL THEN 'Unnamed campaign ' || a.email_campaign_id END) AS campaign_name,
      ec.subject                                                       AS campaign_subject,
      CASE UPPER(ec.type)
      WHEN 'BATCH_EMAIL'               THEN 'One-time send'
      WHEN 'LOCALTIME_EMAIL'           THEN 'One-time send (local time)'
      WHEN 'AB_EMAIL'                  THEN 'A/B test'
      WHEN 'AUTOMATED_EMAIL'           THEN 'Automated (workflow)'
      WHEN 'AUTOMATED_AB_EMAIL'        THEN 'Automated A/B test'
      WHEN 'BLOG_EMAIL'                THEN 'Blog digest'
      WHEN 'RSS_EMAIL'                 THEN 'RSS digest'
      WHEN 'FOLLOWUP_EMAIL'            THEN 'Form follow-up'
      WHEN 'OPTIN_EMAIL'               THEN 'Double opt-in'
      WHEN 'SINGLE_SEND_API'           THEN 'Transactional'
      WHEN 'MARKETING_SINGLE_SEND_API' THEN 'Transactional'
      WHEN 'LEADFLOW_EMAIL'            THEN 'Lead flow'
      ELSE INITCAP(REPLACE(ec.type, '_', ' '))
      END                                                              AS campaign_type_label,
      a.clicked_url,
      a.drop_reason,
      a.subscription_change_source,
      a.form_title,
      a.form_page_url,
      t.touch_number,
      CASE WHEN a.activity_type = 'SENT'                THEN 1 ELSE 0 END AS is_sent,
      CASE WHEN a.activity_type = 'DELIVERED'           THEN 1 ELSE 0 END AS is_delivered,
      CASE WHEN a.activity_type = 'DELIVERED' AND a.type_seq = 1 THEN 1 ELSE 0 END AS is_unique_delivered,
      CASE WHEN a.activity_type = 'BOUNCE'              THEN 1 ELSE 0 END AS is_bounce,
      CASE WHEN a.activity_type = 'DROPPED'             THEN 1 ELSE 0 END AS is_dropped,
      CASE WHEN a.activity_type = 'OPEN'                THEN 1 ELSE 0 END AS is_open,
      CASE WHEN a.activity_type = 'OPEN'  AND a.type_seq = 1 THEN 1 ELSE 0 END AS is_unique_open,
      CASE WHEN a.activity_type = 'CLICK'               THEN 1 ELSE 0 END AS is_click,
      CASE WHEN a.activity_type = 'CLICK' AND a.type_seq = 1 THEN 1 ELSE 0 END AS is_unique_click,
      CASE WHEN a.activity_type = 'FORM_SUBMISSION'     THEN 1 ELSE 0 END AS is_form_submission,
      CASE WHEN a.activity_type = 'FORM_SUBMISSION' AND a.email_campaign_id IS NOT NULL AND a.type_seq = 1
      THEN 1 ELSE 0 END AS is_conversion,
      CASE WHEN a.activity_type = 'SUBSCRIBED'          THEN 1 ELSE 0 END AS is_subscribe,
      CASE WHEN a.activity_type = 'UNSUBSCRIBED'        THEN 1 ELSE 0 END AS is_unsubscribe,
      CASE WHEN a.activity_type = 'REPORTED_SPAM'       THEN 1 ELSE 0 END AS is_spam_report,
      CASE WHEN a.activity_type = 'CLEARED_SPAM_REPORT' THEN 1 ELSE 0 END AS is_spam_cleared,
      CASE WHEN a.activity_type = 'CONTACT_CREATED'     THEN 1 ELSE 0 END AS is_new_contact
      FROM (
      SELECT
      u.*,
      ROW_NUMBER() OVER (PARTITION BY u.recipient_email, u.email_campaign_id, u.activity_type
      ORDER BY u.activity_at, u.activity_id) AS type_seq
      FROM (

      -- 1. Email engagement (deduplicated; STATUSCHANGE / SPAMREPORT come from
      --    the subscription change table instead)
      SELECT
      'ev-' || e.id::VARCHAR                                 AS activity_id,
      'Email engagement'::VARCHAR(30)                        AS activity_source,
      UPPER(e.type)::VARCHAR(40)                             AS activity_type,
      CONVERT_TIMEZONE('UTC', 'America/New_York', e.created) AS activity_at,
      e.email_campaign_id::VARCHAR                           AS email_campaign_id,
      LOWER(TRIM(e.recipient))                               AS recipient_email,
      CASE WHEN UPPER(e.type) = 'CLICK' THEN e.url END       AS clicked_url,
      e.drop_reason::VARCHAR                                 AS drop_reason,
      NULL::VARCHAR                                          AS subscription_change_source,
      NULL::VARCHAR                                          AS form_title,
      NULL::VARCHAR                                          AS form_page_url
      FROM (
      SELECT ee.id, ee.type, ee.created, ee.email_campaign_id, ee.recipient, ee.url, ee.drop_reason,
      ROW_NUMBER() OVER (PARTITION BY ee.id ORDER BY ee.received_at DESC) AS rn
      FROM hubspot.email_events ee
      WHERE ee.received_at >= DATEADD(month, -13, DATE_TRUNC('month', GETDATE()))
      AND ee.created     >= DATEADD(month, -13, DATE_TRUNC('month', GETDATE()))
      AND ee.type NOT IN ('STATUSCHANGE', 'SPAMREPORT', 'statuschange', 'spamreport')
      ) e
      WHERE e.rn = 1

      UNION ALL

      -- 2. Subscription changes. One plain join to the causing email event
      --    (no IN-subquery, no DISTINCT); duplicates removed afterwards.
      SELECT
      'sc-' || sc.id::VARCHAR,
      'Subscription change',
      UPPER(sc.change),
      CONVERT_TIMEZONE('UTC', 'America/New_York', sc.changed_at),
      sc.email_campaign_id,
      LOWER(TRIM(COALESCE(sc.recipient, es.email))),
      NULL::VARCHAR, NULL::VARCHAR, sc.source::VARCHAR, NULL::VARCHAR, NULL::VARCHAR
      FROM (
      SELECT
      c.id, c.change, c.source, c.email_subscription_event_id,
      COALESCE(c."timestamp", c.caused_by_event_created) AS changed_at,
      ev.email_campaign_id::VARCHAR                       AS email_campaign_id,
      ev.recipient,
      ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.received_at DESC) AS rn
      FROM hubspot.email_subscription_event_changes c
      LEFT JOIN (
      SELECT id, email_campaign_id, recipient
      FROM hubspot.email_events
      WHERE received_at >= DATEADD(month, -14, DATE_TRUNC('month', GETDATE()))
      ) ev
      ON ev.id::VARCHAR = c.caused_by_event_id::VARCHAR
      WHERE c.change IS NOT NULL
      AND c.received_at >= DATEADD(month, -13, DATE_TRUNC('month', GETDATE()))
      AND COALESCE(c."timestamp", c.caused_by_event_created) >= DATEADD(month, -13, DATE_TRUNC('month', GETDATE()))
      ) sc
      LEFT JOIN hubspot.email_subscriptions es
      ON es.id::VARCHAR = sc.email_subscription_event_id::VARCHAR
      WHERE sc.rn = 1

      UNION ALL

      -- 3. Form submissions. CONTACT LINK: form_submissions has no contact_id in
      --    this warehouse, so submissions can't be credited to an email yet and
      --    the click lookup is skipped (it could never match and was pure cost).
      --    Once the linking column is known, ask to re-enable last-click
      --    attribution here; Conversions stay 0 until then.
      SELECT
      'fs-' || fs.id::VARCHAR, 'Form submission', 'FORM_SUBMISSION',
      CONVERT_TIMEZONE('UTC', 'America/New_York', fs."timestamp"),
      NULL::VARCHAR, NULL::VARCHAR,
      NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, fs.title::VARCHAR, fs.page_url::VARCHAR
      FROM hubspot.form_submissions fs
      WHERE fs."timestamp" >= DATEADD(month, -13, DATE_TRUNC('month', GETDATE()))

      UNION ALL

      -- 4. New contacts (valid, un-merged records)
      SELECT
      'ct-' || c.id::VARCHAR, 'Contact', 'CONTACT_CREATED',
      CONVERT_TIMEZONE('UTC', 'America/New_York', c.added_at),
      NULL::VARCHAR, LOWER(TRIM(c.email)),
      NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR
      FROM hubspot.contacts c
      WHERE COALESCE(c.is_contact, TRUE)
      AND (c.canonical_vid IS NULL OR c.canonical_vid::VARCHAR = c.id::VARCHAR)
      AND c.added_at >= DATEADD(month, -13, DATE_TRUNC('month', GETDATE()))
      ) u
      ) a

      -- Touch number: rank campaigns per person on a small grouped table (all
      -- sends ever, only 3 columns), then join back. 1 = first campaign received.
      LEFT JOIN (
      SELECT
      s.recipient_email,
      s.email_campaign_id,
      DENSE_RANK() OVER (PARTITION BY s.recipient_email
      ORDER BY s.first_sent_at, s.email_campaign_id) AS touch_number
      FROM (
      SELECT LOWER(TRIM(recipient))     AS recipient_email,
      email_campaign_id::VARCHAR AS email_campaign_id,
      MIN(created)               AS first_sent_at
      FROM hubspot.email_events
      WHERE type IN ('SENT', 'sent')
      GROUP BY 1, 2
      ) s
      ) t
      ON  t.recipient_email   = a.recipient_email
      AND t.email_campaign_id = a.email_campaign_id

      LEFT JOIN hubspot.email_campaigns ec
      ON ec.id::VARCHAR = a.email_campaign_id
      ;;
  }

  # ===========================================================================
  # PERIOD-OVER-PERIOD CONTROLS
  # ===========================================================================

  filter: reporting_period {
    type: date
    label: "Reporting Period"
    group_label: "Period Comparison"
    description: "The date range to report on, for example 'last 30 days' or 'this quarter'. All standard measures (Emails Sent, Open Rate, Unsubscribes...) are calculated for this range. Pair with Compare To to see the change against an earlier period."
  }

  parameter: compare_to {
    type: unquoted
    label: "Compare To"
    group_label: "Period Comparison"
    description: "Which earlier window to compare the Reporting Period against. 'Previous period' uses the same number of days immediately before; the others shift the Reporting Period back a month, a quarter or a year (useful for seasonal comparisons such as Christmas movie campaigns year over year)."
    allowed_value: { label: "Previous period"  value: "previous_period" }
    allowed_value: { label: "Previous month"   value: "previous_month" }
    allowed_value: { label: "Previous quarter" value: "previous_quarter" }
    allowed_value: { label: "Previous year"    value: "previous_year" }
    default_value: "previous_period"
  }

  # --- Window boundaries: constants built from the filter, so comparisons
  #     against activity_at stay direct and use the sort key ----------------

  dimension: period_start {
    hidden: yes
    type: date_time
    convert_tz: no
    sql: {% if hubspot_email_activity.reporting_period._is_filtered %} CAST({% date_start reporting_period %} AS TIMESTAMP) {% else %} CAST(NULL AS TIMESTAMP) {% endif %} ;;
  }

  dimension: period_end {
    hidden: yes
    type: date_time
    convert_tz: no
    sql: {% if hubspot_email_activity.reporting_period._is_filtered %} CAST({% date_end reporting_period %} AS TIMESTAMP) {% else %} CAST(NULL AS TIMESTAMP) {% endif %} ;;
  }

  dimension: prior_start {
    hidden: yes
    type: date_time
    convert_tz: no
    sql:
      {% if hubspot_email_activity.compare_to._parameter_value == 'previous_year' %}      DATEADD(year,  -1, ${period_start})
      {% elsif hubspot_email_activity.compare_to._parameter_value == 'previous_quarter' %} DATEADD(month, -3, ${period_start})
      {% elsif hubspot_email_activity.compare_to._parameter_value == 'previous_month' %}   DATEADD(month, -1, ${period_start})
      {% else %} DATEADD(second, -1 * DATEDIFF(second, ${period_start}, ${period_end}), ${period_start})
      {% endif %} ;;
  }

  dimension: prior_end {
    hidden: yes
    type: date_time
    convert_tz: no
    sql:
      {% if hubspot_email_activity.compare_to._parameter_value == 'previous_year' %}      DATEADD(year,  -1, ${period_end})
      {% elsif hubspot_email_activity.compare_to._parameter_value == 'previous_quarter' %} DATEADD(month, -3, ${period_end})
      {% elsif hubspot_email_activity.compare_to._parameter_value == 'previous_month' %}   DATEADD(month, -1, ${period_end})
      {% else %} ${period_start}
      {% endif %} ;;
  }

  # 1/0 multipliers used by every measure (hidden SQL helpers)
  dimension: current_flag {
    hidden: yes
    type: number
    sql: {% if hubspot_email_activity.reporting_period._is_filtered %} CASE WHEN ${TABLE}.activity_at >= ${period_start} AND ${TABLE}.activity_at < ${period_end} THEN 1 ELSE 0 END {% else %} 1 {% endif %} ;;
  }

  dimension: prior_flag {
    hidden: yes
    type: number
    sql: {% if hubspot_email_activity.reporting_period._is_filtered %} CASE WHEN ${TABLE}.activity_at >= ${prior_start} AND ${TABLE}.activity_at < ${prior_end} THEN 1 ELSE 0 END {% else %} 0 {% endif %} ;;
  }

  # --- User-facing period fields ---------------------------------------------

  dimension: is_current_period {
    type: yesno
    label: "Is Reporting Period"
    group_label: "Period Comparison"
    description: "Yes when the activity happened inside the selected Reporting Period."
    sql: ${current_flag} = 1 ;;
  }

  dimension: is_prior_period {
    type: yesno
    label: "Is Prior Period"
    group_label: "Period Comparison"
    description: "Yes when the activity happened inside the comparison window chosen in Compare To."
    sql: ${prior_flag} = 1 ;;
  }

  dimension: period {
    type: string
    label: "Period"
    group_label: "Period Comparison"
    description: "Labels each row 'This period' or 'Prior period' so the two windows can be shown side by side. Shows 'All time' when no Reporting Period is selected."
    sql: {% if hubspot_email_activity.reporting_period._is_filtered %} CASE WHEN ${current_flag} = 1 THEN 'This period' WHEN ${prior_flag} = 1 THEN 'Prior period' END {% else %} 'All time' {% endif %} ;;
  }

  dimension: days_into_period {
    type: number
    label: "Day of Period"
    group_label: "Period Comparison"
    description: "Day number within its window (0 = first day). Lines this period and the prior period up day by day."
    sql: CASE WHEN ${current_flag} = 1 THEN DATEDIFF(day, ${period_start}, ${TABLE}.activity_at)
      WHEN ${prior_flag}   = 1 THEN DATEDIFF(day, ${prior_start},  ${TABLE}.activity_at) END ;;
  }

  dimension_group: period_aligned {
    type: time
    label: "Comparison"
    group_label: "Period Comparison"
    description: "Calendar date inside the Reporting Period. Prior-period activity is moved forward onto the matching day, so trend charts can show this period and the prior period as two lines on one axis. Use with a measure and its '(Prior Period)' twin."
    timeframes: [date, week, month]
    convert_tz: no
    datatype: timestamp
    sql: {% if hubspot_email_activity.reporting_period._is_filtered %} DATEADD(day, ${days_into_period}, DATE_TRUNC('day', ${period_start})) {% else %} ${TABLE}.activity_at {% endif %} ;;
  }

  # ===========================================================================
  # DIMENSIONS
  # ===========================================================================

  dimension: activity_id {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.activity_id ;;
  }

  dimension_group: activity {
    type: time
    label: "Activity"
    description: "When the email was sent, opened or clicked, when someone subscribed or unsubscribed, when a form was submitted, or when a contact was created. US Eastern time."
    timeframes: [raw, time, hour_of_day, date, day_of_week, day_of_week_index, week, month, quarter, year]
    convert_tz: no
    datatype: timestamp
    sql: ${TABLE}.activity_at ;;
  }

  dimension: activity_source {
    type: string
    label: "Activity Category"
    description: "Broad category: Email engagement, Subscription change, Form submission or Contact."
    sql: ${TABLE}.activity_source ;;
  }

  dimension: activity_type {
    type: string
    label: "Activity Type Code"
    description: "HubSpot's raw code (SENT, DELIVERED, OPEN, CLICK, BOUNCE, SUBSCRIBED, UNSUBSCRIBED, REPORTED_SPAM, FORM_SUBMISSION, CONTACT_CREATED...). Use Activity Type for readable names."
    sql: ${TABLE}.activity_type ;;
  }

  dimension: activity_type_label {
    type: string
    label: "Activity Type"
    description: "What happened, in plain language: Sent, Delivered, Opened, Clicked, Bounced, Subscribed, Unsubscribed, Reported as spam, Submitted a form, New contact."
    sql: CASE ${activity_type}
           WHEN 'SENT' THEN 'Sent'               WHEN 'PROCESSED' THEN 'Processed'
           WHEN 'DELIVERED' THEN 'Delivered'     WHEN 'DEFERRED' THEN 'Delayed'
           WHEN 'DROPPED' THEN 'Dropped (not sent)' WHEN 'SUPPRESSED' THEN 'Suppressed'
           WHEN 'BOUNCE' THEN 'Bounced'          WHEN 'OPEN' THEN 'Opened'
           WHEN 'CLICK' THEN 'Clicked'           WHEN 'FORWARD' THEN 'Forwarded'
           WHEN 'PRINT' THEN 'Printed'           WHEN 'SUBSCRIBED' THEN 'Subscribed'
           WHEN 'UNSUBSCRIBED' THEN 'Unsubscribed' WHEN 'REPORTED_SPAM' THEN 'Reported as spam'
           WHEN 'CLEARED_SPAM_REPORT' THEN 'Spam report cleared' WHEN 'BOUNCED' THEN 'Marked as bounced'
           WHEN 'FORM_SUBMISSION' THEN 'Submitted a form' WHEN 'CONTACT_CREATED' THEN 'New contact'
           ELSE INITCAP(REPLACE(${activity_type}, '_', ' '))
         END ;;
  }

  dimension: email_campaign_id {
    type: string
    label: "Campaign ID"
    description: "HubSpot's ID for the email campaign. Use Campaign Name for reporting; use this to find the campaign in HubSpot."
    sql: ${TABLE}.email_campaign_id ;;
  }

  dimension: campaign_name {
    type: string
    label: "Campaign Name"
    group_label: "Campaign"
    description: "The campaign's name in HubSpot, for example 'Heartland Season 17 Premiere' or 'Trial Day 3 Reminder'. Several A/B variants or automated emails can share one name. Empty for activity not tied to an email (new contacts, unattributed form submissions)."
    sql: ${TABLE}.campaign_name ;;
    # Optional drill once your Campaign Lookup dashboard exists:
    # link: { label: "Open in Campaign Lookup" url: "/dashboards/<dashboard_id>?Campaign+Name={{ value | url_encode }}" }
  }

  dimension: campaign_subject {
    type: string
    label: "Subject Line"
    group_label: "Campaign"
    description: "The subject line recipients saw in their inbox. Useful for comparing which subject lines earn higher Open Rates."
    sql: ${TABLE}.campaign_subject ;;
  }

  dimension: campaign_type_label {
    type: string
    label: "Campaign Type"
    group_label: "Campaign"
    description: "What kind of email this is: One-time send (newsletters, premieres), A/B test, Automated (workflows such as trial onboarding or winback), Blog or RSS digest, Form follow-up, or Transactional."
    sql: ${TABLE}.campaign_type_label ;;
  }

  dimension: recipient_email {
    type: string
    label: "Recipient Email"
    description: "Email address of the person who received the email or took the action. Personal data: do not export or share outside the marketing team."
    tags: ["pii", "email"]
    sql: ${TABLE}.recipient_email ;;
  }

  dimension: touch_number {
    type: number
    label: "Touch Number"
    description: "Which campaign this was in the recipient's history: 1 = the first campaign they ever received from us, 2 = the second, and so on. Shows how engagement changes as subscribers receive more emails."
    sql: ${TABLE}.touch_number ;;
  }

  dimension: touch_number_tier {
    type: tier
    label: "Touch Number (Grouped)"
    description: "Touch Number grouped into ranges (1, 2, 3... up to 10+), for funnel-by-touch charts."
    tiers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    style: integer
    sql: ${touch_number} ;;
  }

  dimension: clicked_url {
    type: string
    label: "Clicked Link"
    description: "The link a recipient clicked in the email (clicks only). Shows which titles, offers or pages drove interest."
    sql: ${TABLE}.clicked_url ;;
  }

  dimension: drop_reason {
    type: string
    label: "Drop Reason"
    description: "Why HubSpot did not send an email (for example previously bounced or unsubscribed). Dropped activity only."
    sql: ${TABLE}.drop_reason ;;
  }

  dimension: subscription_change_source {
    type: string
    label: "Subscription Change Source"
    description: "Where a subscribe, unsubscribe or spam report came from, as reported by HubSpot (email footer link, form, import, manual change...)."
    sql: ${TABLE}.subscription_change_source ;;
  }

  dimension: form_title {
    type: string
    label: "Form Name"
    description: "Name of the HubSpot form that was submitted (form submissions only)."
    sql: ${TABLE}.form_title ;;
  }

  dimension: form_page_url {
    type: string
    label: "Form Page URL"
    description: "Web page the form was submitted from (form submissions only)."
    sql: ${TABLE}.form_page_url ;;
  }

  dimension: is_email_attributed {
    type: yesno
    label: "Is Credited to Email"
    description: "Yes when the activity is tied to an email campaign. For form submissions, yes means the person clicked an email in the 7 days before submitting."
    sql: ${TABLE}.email_campaign_id IS NOT NULL ;;
  }

  # ===========================================================================
  # MEASURES: REPORTING PERIOD (plain names = the selected Reporting Period,
  # or all time when no Reporting Period is set)
  # ===========================================================================

  measure: campaigns_sent {
    type: count_distinct
    label: "Campaigns Sent"
    group_label: "Email Volume"
    description: "Number of different email campaigns that sent at least one email."
    sql: CASE WHEN ${current_flag} = 1 AND ${TABLE}.is_sent = 1 THEN ${TABLE}.email_campaign_id END ;;
  }

  measure: emails_sent {
    type: sum
    label: "Emails Sent"
    group_label: "Email Volume"
    description: "Total emails sent. Also called sends or volume."
    sql: ${current_flag} * ${TABLE}.is_sent ;;
    value_format_name: decimal_0
  }

  measure: emails_delivered {
    type: sum
    label: "Emails Delivered"
    group_label: "Email Volume"
    description: "Emails that reached the recipient’s mail server (sent minus bounces)."
    sql: ${current_flag} * ${TABLE}.is_delivered ;;
    value_format_name: decimal_0
  }

  measure: recipients_delivered {
    type: sum
    label: "Recipients Delivered"
    group_label: "Email Volume"
    description: "People a campaign was delivered to, counted once per campaign. The base for Open, Click and Conversion Rate."
    sql: ${current_flag} * ${TABLE}.is_unique_delivered ;;
    value_format_name: decimal_0
  }

  measure: emails_bounced {
    type: sum
    label: "Emails Bounced"
    group_label: "Email Volume"
    description: "Emails rejected by the recipient’s mail server."
    sql: ${current_flag} * ${TABLE}.is_bounce ;;
    value_format_name: decimal_0
  }

  measure: emails_dropped {
    type: sum
    label: "Emails Dropped"
    group_label: "Email Volume"
    description: "Emails HubSpot chose not to send (for example to people who previously bounced or unsubscribed)."
    sql: ${current_flag} * ${TABLE}.is_dropped ;;
    value_format_name: decimal_0
  }

  measure: unique_opens {
    type: sum
    label: "Unique Opens"
    group_label: "Engagement"
    description: "People who opened a campaign, counted once per campaign on their first open. Also called openers."
    sql: ${current_flag} * ${TABLE}.is_unique_open ;;
    value_format_name: decimal_0
  }

  measure: total_opens {
    type: sum
    label: "Total Opens"
    group_label: "Engagement"
    description: "Every open, including repeat opens by the same person."
    sql: ${current_flag} * ${TABLE}.is_open ;;
    value_format_name: decimal_0
  }

  measure: unique_clicks {
    type: sum
    label: "Unique Clicks"
    group_label: "Engagement"
    description: "People who clicked a link in a campaign, counted once per campaign on their first click. Also called clickers."
    sql: ${current_flag} * ${TABLE}.is_unique_click ;;
    value_format_name: decimal_0
  }

  measure: total_clicks {
    type: sum
    label: "Total Clicks"
    group_label: "Engagement"
    description: "Every link click, including repeat clicks."
    sql: ${current_flag} * ${TABLE}.is_click ;;
    value_format_name: decimal_0
  }

  measure: conversions {
    type: sum
    label: "Conversions"
    group_label: "Conversions"
    description: "People who submitted a HubSpot form within 7 days of clicking a campaign email, counted once per campaign. Credit goes to the last email clicked."
    sql: ${current_flag} * ${TABLE}.is_conversion ;;
    value_format_name: decimal_0
  }

  measure: form_submissions {
    type: sum
    label: "Form Submissions (All)"
    group_label: "Conversions"
    description: "All HubSpot form submissions, whether or not an email led to them."
    sql: ${current_flag} * ${TABLE}.is_form_submission ;;
    value_format_name: decimal_0
  }

  measure: subscribes {
    type: sum
    label: "Subscribes"
    group_label: "Subscriptions"
    description: "Times someone subscribed or re-subscribed to email."
    sql: ${current_flag} * ${TABLE}.is_subscribe ;;
    value_format_name: decimal_0
  }

  measure: unsubscribes {
    type: sum
    label: "Unsubscribes"
    group_label: "Subscriptions"
    description: "Times someone unsubscribed from email. Also called opt-outs."
    sql: ${current_flag} * ${TABLE}.is_unsubscribe ;;
    value_format_name: decimal_0
  }

  measure: spam_reports {
    type: sum
    label: "Spam Reports"
    group_label: "Subscriptions"
    description: "Times a recipient marked an email as spam. High numbers hurt inbox placement for every send."
    sql: ${current_flag} * ${TABLE}.is_spam_report ;;
    value_format_name: decimal_0
  }

  measure: spam_reports_cleared {
    type: sum
    label: "Spam Reports Cleared"
    group_label: "Subscriptions"
    description: "Spam reports that were later cleared."
    sql: ${current_flag} * ${TABLE}.is_spam_cleared ;;
    value_format_name: decimal_0
  }

  measure: new_contacts {
    type: sum
    label: "New Contacts"
    group_label: "Contacts"
    description: "Contacts added to HubSpot. Also called new leads."
    sql: ${current_flag} * ${TABLE}.is_new_contact ;;
    value_format_name: decimal_0
  }

  measure: net_subscriptions {
    type: number
    label: "Net Subscriptions"
    group_label: "Subscriptions"
    description: "Subscribes minus unsubscribes. Positive means the email audience grew."
    sql: ${subscribes} - ${unsubscribes} ;;
    value_format_name: decimal_0
  }

  measure: unsubscribes_as_negative {
    type: number
    label: "Unsubscribes (Shown Below Zero)"
    group_label: "Subscriptions"
    description: "Unsubscribes as a negative number, for charts that show audience gains above zero and losses below."
    sql: -1 * ${unsubscribes} ;;
    value_format_name: decimal_0
  }

  measure: spam_reports_as_negative {
    type: number
    label: "Spam Reports (Shown Below Zero)"
    group_label: "Subscriptions"
    description: "Spam reports as a negative number, for gains-and-losses charts."
    sql: -1 * ${spam_reports} ;;
    value_format_name: decimal_0
  }

  measure: delivery_rate {
    type: number
    label: "Delivery Rate"
    group_label: "Rates"
    description: "Share of sent emails that were delivered."
    sql: 1.0 * ${emails_delivered} / NULLIF(${emails_sent}, 0) ;;
    value_format_name: percent_1
  }

  measure: bounce_rate {
    type: number
    label: "Bounce Rate"
    group_label: "Rates"
    description: "Share of sent emails that bounced. Keep under 2%."
    sql: 1.0 * ${emails_bounced} / NULLIF(${emails_sent}, 0) ;;
    value_format_name: percent_2
  }

  measure: open_rate {
    type: number
    label: "Open Rate"
    group_label: "Rates"
    description: "Share of people reached who opened. Apple Mail privacy features inflate opens, so compare trends rather than absolute values."
    sql: 1.0 * ${unique_opens} / NULLIF(${recipients_delivered}, 0) ;;
    value_format_name: percent_1
  }

  measure: click_rate {
    type: number
    label: "Click Rate"
    group_label: "Rates"
    description: "Share of people reached who clicked. Also called click-through rate or CTR."
    sql: 1.0 * ${unique_clicks} / NULLIF(${recipients_delivered}, 0) ;;
    value_format_name: percent_2
  }

  measure: click_to_open_rate {
    type: number
    label: "Click-to-Open Rate"
    group_label: "Rates"
    description: "Of the people who opened, the share who clicked. Measures how compelling the content was. Also called CTOR."
    sql: 1.0 * ${unique_clicks} / NULLIF(${unique_opens}, 0) ;;
    value_format_name: percent_1
  }

  measure: conversion_rate {
    type: number
    label: "Conversion Rate"
    group_label: "Rates"
    description: "Share of people reached who went on to submit a form within 7 days of clicking."
    sql: 1.0 * ${conversions} / NULLIF(${recipients_delivered}, 0) ;;
    value_format_name: percent_2
  }

  measure: unsubscribe_rate {
    type: number
    label: "Unsubscribe Rate"
    group_label: "Rates"
    description: "Share of people reached who unsubscribed. Under 0.5% per send is healthy."
    sql: 1.0 * ${unsubscribes} / NULLIF(${recipients_delivered}, 0) ;;
    value_format_name: percent_2
  }

  measure: spam_report_rate {
    type: number
    label: "Spam Report Rate"
    group_label: "Rates"
    description: "Share of people reached who reported spam. Mailbox providers start filtering above about 0.1%."
    sql: 1.0 * ${spam_reports} / NULLIF(${recipients_delivered}, 0) ;;
    value_format_name: percent_3
  }

  # ===========================================================================
  # MEASURES: PRIOR PERIOD (the window chosen in Compare To)
  # ===========================================================================

  measure: campaigns_sent_prior {
    type: count_distinct
    label: "Campaigns Sent (Prior Period)"
    group_label: "Prior Period"
    description: "Campaigns Sent in the comparison window."
    sql: CASE WHEN ${prior_flag} = 1 AND ${TABLE}.is_sent = 1 THEN ${TABLE}.email_campaign_id END ;;
  }

  measure: emails_sent_prior {
    type: sum
    label: "Emails Sent (Prior Period)"
    group_label: "Prior Period"
    description: "Emails Sent in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_sent ;;
    value_format_name: decimal_0
  }

  measure: emails_delivered_prior {
    type: sum
    label: "Emails Delivered (Prior Period)"
    group_label: "Prior Period"
    description: "Emails Delivered in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_delivered ;;
    value_format_name: decimal_0
  }

  measure: recipients_delivered_prior {
    type: sum
    label: "Recipients Delivered (Prior Period)"
    group_label: "Prior Period"
    description: "Recipients Delivered in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_unique_delivered ;;
    value_format_name: decimal_0
  }

  measure: unique_opens_prior {
    type: sum
    label: "Unique Opens (Prior Period)"
    group_label: "Prior Period"
    description: "Unique Opens in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_unique_open ;;
    value_format_name: decimal_0
  }

  measure: unique_clicks_prior {
    type: sum
    label: "Unique Clicks (Prior Period)"
    group_label: "Prior Period"
    description: "Unique Clicks in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_unique_click ;;
    value_format_name: decimal_0
  }

  measure: conversions_prior {
    type: sum
    label: "Conversions (Prior Period)"
    group_label: "Prior Period"
    description: "Conversions in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_conversion ;;
    value_format_name: decimal_0
  }

  measure: subscribes_prior {
    type: sum
    label: "Subscribes (Prior Period)"
    group_label: "Prior Period"
    description: "Subscribes in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_subscribe ;;
    value_format_name: decimal_0
  }

  measure: unsubscribes_prior {
    type: sum
    label: "Unsubscribes (Prior Period)"
    group_label: "Prior Period"
    description: "Unsubscribes in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_unsubscribe ;;
    value_format_name: decimal_0
  }

  measure: spam_reports_prior {
    type: sum
    label: "Spam Reports (Prior Period)"
    group_label: "Prior Period"
    description: "Spam Reports in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_spam_report ;;
    value_format_name: decimal_0
  }

  measure: new_contacts_prior {
    type: sum
    label: "New Contacts (Prior Period)"
    group_label: "Prior Period"
    description: "New Contacts in the comparison window."
    sql: ${prior_flag} * ${TABLE}.is_new_contact ;;
    value_format_name: decimal_0
  }

  measure: net_subscriptions_prior {
    type: number
    label: "Net Subscriptions (Prior Period)"
    group_label: "Prior Period"
    description: "Net Subscriptions in the comparison window."
    sql: ${subscribes_prior} - ${unsubscribes_prior} ;;
    value_format_name: decimal_0
  }

  measure: open_rate_prior {
    type: number
    label: "Open Rate (Prior Period)"
    group_label: "Prior Period"
    description: "Open Rate in the comparison window."
    sql: 1.0 * ${unique_opens_prior} / NULLIF(${recipients_delivered_prior}, 0) ;;
    value_format_name: percent_1
  }

  measure: click_rate_prior {
    type: number
    label: "Click Rate (Prior Period)"
    group_label: "Prior Period"
    description: "Click Rate in the comparison window."
    sql: 1.0 * ${unique_clicks_prior} / NULLIF(${recipients_delivered_prior}, 0) ;;
    value_format_name: percent_2
  }

  measure: conversion_rate_prior {
    type: number
    label: "Conversion Rate (Prior Period)"
    group_label: "Prior Period"
    description: "Conversion Rate in the comparison window."
    sql: 1.0 * ${conversions_prior} / NULLIF(${recipients_delivered_prior}, 0) ;;
    value_format_name: percent_2
  }

  measure: unsubscribe_rate_prior {
    type: number
    label: "Unsubscribe Rate (Prior Period)"
    group_label: "Prior Period"
    description: "Unsubscribe Rate in the comparison window."
    sql: 1.0 * ${unsubscribes_prior} / NULLIF(${recipients_delivered_prior}, 0) ;;
    value_format_name: percent_2
  }

  # ===========================================================================
  # MEASURES: CHANGE VS PRIOR PERIOD
  # Counts show % change. Rates show the change in percentage points (pts):
  # an open rate of 31.0% vs 29.5% is +1.5 pts.
  # ===========================================================================

  measure: campaigns_sent_change_pct {
    type: number
    label: "Campaigns Sent Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in Campaigns Sent versus the comparison window."
    sql: 1.0 * (${campaigns_sent} - ${campaigns_sent_prior}) / NULLIF(${campaigns_sent_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: emails_sent_change_pct {
    type: number
    label: "Emails Sent Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in Emails Sent versus the comparison window."
    sql: 1.0 * (${emails_sent} - ${emails_sent_prior}) / NULLIF(${emails_sent_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: emails_delivered_change_pct {
    type: number
    label: "Emails Delivered Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in Emails Delivered versus the comparison window."
    sql: 1.0 * (${emails_delivered} - ${emails_delivered_prior}) / NULLIF(${emails_delivered_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: unique_opens_change_pct {
    type: number
    label: "Unique Opens Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in Unique Opens versus the comparison window."
    sql: 1.0 * (${unique_opens} - ${unique_opens_prior}) / NULLIF(${unique_opens_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: unique_clicks_change_pct {
    type: number
    label: "Unique Clicks Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in Unique Clicks versus the comparison window."
    sql: 1.0 * (${unique_clicks} - ${unique_clicks_prior}) / NULLIF(${unique_clicks_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: conversions_change_pct {
    type: number
    label: "Conversions Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in Conversions versus the comparison window."
    sql: 1.0 * (${conversions} - ${conversions_prior}) / NULLIF(${conversions_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: subscribes_change_pct {
    type: number
    label: "Subscribes Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in Subscribes versus the comparison window."
    sql: 1.0 * (${subscribes} - ${subscribes_prior}) / NULLIF(${subscribes_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: unsubscribes_change_pct {
    type: number
    label: "Unsubscribes Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in Unsubscribes versus the comparison window. A rise is bad news."
    sql: 1.0 * (${unsubscribes} - ${unsubscribes_prior}) / NULLIF(${unsubscribes_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: spam_reports_change_pct {
    type: number
    label: "Spam Reports Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in Spam Reports versus the comparison window. A rise is bad news."
    sql: 1.0 * (${spam_reports} - ${spam_reports_prior}) / NULLIF(${spam_reports_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: new_contacts_change_pct {
    type: number
    label: "New Contacts Change %"
    group_label: "Change vs Prior Period"
    description: "Percent change in New Contacts versus the comparison window."
    sql: 1.0 * (${new_contacts} - ${new_contacts_prior}) / NULLIF(${new_contacts_prior}, 0) ;;
    value_format: "+0.0%;-0.0%;0.0%"
  }

  measure: net_subscriptions_change {
    type: number
    label: "Net Subscriptions Change"
    group_label: "Change vs Prior Period"
    description: "Difference in Net Subscriptions versus the comparison window (a count, because net subscriptions can be negative)."
    sql: ${net_subscriptions} - ${net_subscriptions_prior} ;;
    value_format: "+#,##0;-#,##0;0"
  }

  measure: open_rate_change_pts {
    type: number
    label: "Open Rate Change (pts)"
    group_label: "Change vs Prior Period"
    description: "Open Rate minus the prior period’s Open Rate, in percentage points."
    sql: 100.0 * (${open_rate} - ${open_rate_prior}) ;;
    value_format: "+0.0\" pts\";-0.0\" pts\";0.0\" pts\""
  }

  measure: click_rate_change_pts {
    type: number
    label: "Click Rate Change (pts)"
    group_label: "Change vs Prior Period"
    description: "Click Rate minus the prior period’s Click Rate, in percentage points."
    sql: 100.0 * (${click_rate} - ${click_rate_prior}) ;;
    value_format: "+0.00\" pts\";-0.00\" pts\";0.00\" pts\""
  }

  measure: conversion_rate_change_pts {
    type: number
    label: "Conversion Rate Change (pts)"
    group_label: "Change vs Prior Period"
    description: "Conversion Rate minus the prior period’s Conversion Rate, in percentage points."
    sql: 100.0 * (${conversion_rate} - ${conversion_rate_prior}) ;;
    value_format: "+0.00\" pts\";-0.00\" pts\";0.00\" pts\""
  }

  measure: unsubscribe_rate_change_pts {
    type: number
    label: "Unsubscribe Rate Change (pts)"
    group_label: "Change vs Prior Period"
    description: "Unsubscribe Rate minus the prior period’s Unsubscribe Rate, in percentage points. A rise is bad news."
    sql: 100.0 * (${unsubscribe_rate} - ${unsubscribe_rate_prior}) ;;
    value_format: "+0.00\" pts\";-0.00\" pts\";0.00\" pts\""
  }
}
