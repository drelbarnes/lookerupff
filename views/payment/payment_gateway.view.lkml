# =============================================================================
# payment_gateway.view.lkml  (Redshift)
# Payment Gateway dashboard + Looker Conversational Analytics
#
# One row per event, four event types:
#   payment_succeeded | payment_failed | subscription_renewed | involuntary_churn
#   checkout_started | checkout_card_error | order_completed   (web checkout, Segment)
#
# Grain notes
#   - Payment events are grouped into "attempt groups" (invoice_id, falling back
#     to subscription_id). Invoice-level measures (first-attempt auth rate,
#     recovery rate, revenue at risk) count only the FIRST attempt row of each
#     invoice, so each invoice is dated by its first attempt.
#   - Dunning / renewal / churn measures count distinct customers.
#   - Checkout measures count distinct anonymous_id (browser visitor). A visitor
#     who errors on one device and orders on another counts as not recovered.
#     Checkout rows carry no gateway, billing period, signup model or customer
#     type, so checkout tiles should not listen to those dashboard filters.
#
# -----------------------------------------------------------------------------
# CONVERSATIONAL ANALYTICS SETUP
# -----------------------------------------------------------------------------
# How this file is tuned for the agent:
#   - Every visible field has a plain-English label and a description that
#     states what it is, how it is calculated, its valid values, and the
#     synonyms business users say ("approval rate", "failed payments", ...).
#   - Ratio measures say "do not sum or average"; the agent should re-query
#     at the grain the user asks for instead of rolling up percentages.
#   - Raw IDs and plumbing columns are hidden; high-cardinality fields
#     (error message, customer ID) point the agent to the grouped version.
#   - Period-over-period fields depend on Liquid date filters the agent
#     cannot set reliably, so they live in set: pop_fields and are excluded
#     from the conversational explore below. For "vs last week" questions the
#     agent uses Report Week / Report Date instead.
#
# Suggested model file entries (not part of this view):
#
#   explore: payment_gateway {                       # dashboard explore
#     label: "Payment Gateway"
#   }
#
#   explore: payment_gateway_ca {                    # point the CA agent here
#     from: payment_gateway
#     view_name: payment_gateway
#     label: "Payment Gateway (Conversational)"
#     description: "Card payment authorization, retries/dunning, recovery and involuntary churn for UP Faith & Family web subscriptions, by gateway (Braintree, PayPal, Stripe) and billing period (monthly, yearly)."
#     fields: [payment_gateway.ca_fields*]
#     always_filter: { filters: [payment_gateway.report_date: "28 days"] }
#   }
#
# Suggested agent instructions (paste into the CA agent):
#   Vocabulary
#     - "auth rate", "approval rate", "acceptance rate", "success rate" ->
#       First-Attempt Authorization Rate unless the user says "after retries"
#       or "eventual" -> Eventual Authorization Rate.
#     - "declines", "failed payments", "failures" -> Failed Charge Attempts.
#     - "dunning", "past due", "payment issues" -> New Dunning Customers /
#       Dunning Rate.
#     - "involuntary churn", "failed-payment churn", "lost to declines" ->
#       Involuntary Churn Customers.
#     - "recovered", "saved", "won back" -> Recovered Invoices / Recovery Rate.
#     - "PayPal", "Braintree", "Stripe" -> Gateway filter (lower-case values).
#     - "monthly"/"annual"/"yearly" -> Billing Period (values: monthly, yearly).
#     - "new customers", "first charge" -> Customer Type = new.
#     - "trial", "trialists", "trial conversion" -> Signup Model = free_trial
#       (only exists for subscriptions created before 2026-09-09).
#     - "paid-only", "no-trial", "since the switch" -> Signup Model = paid_only.
#     - "first renewal", "month 2", "first rebill" -> Lifecycle Stage =
#       First renewal.
#   Business context
#     - UP Faith & Family is a streaming subscription service. On 2026-09-09
#       signup moved from a free trial to paid-only (no trial). New-customer
#       authorization, dunning and churn before and after that date are not
#       comparable; split by Signup Model or keep both periods on the same
#       side of 2026-09-09, and say so when a comparison spans the switch.
#     - Under paid-only, card failures at signup may stop checkout before a
#       subscription exists, so Initial charge auth rates can look higher
#       than under the trial model.
#   Aggregation rules
#     - Never sum or average a rate across rows; re-query the rate at the
#       grain asked for.
#     - Default to weekly grain (Report Week) and the last 4 complete weeks.
#     - For "this week vs last week", query Report Week for the two most
#       recent complete weeks; exclude today's partial day.
#     - Recovery and Eventual Authorization rates are understated for the
#       last ~30 days because invoices are still in retry; say so when
#       reporting them for recent periods.
#   Root cause
#     - When a rate moves, break it down by Gateway, then Decline Category,
#       then Billing Period and Customer Type; report the segment with the
#       largest change in volume-weighted terms.
#     - Use Decline Category, not Decline Message, unless asked for raw text.
#   PII
#     - Do not list individual Customer IDs, Subscription IDs or Invoice IDs
#       unless the user explicitly asks for a record-level drill.
#
# -----------------------------------------------------------------------------
# Columns to verify before the first build (assumed from Chargebee webhook
# flattening; everything else is copied from the existing queries):
#   payment_succeeded / payment_failed:
#     id, content_invoice_id, content_transaction_gateway,
#     content_transaction_amount (cents), content_transaction_currency_code,
#     content_card_card_type, content_card_funding_type
#   payment_succeeded / subscription_renewed:
#     content_subscription_subscription_items_0_item_price_id
#   payment_succeeded / payment_failed / subscription_renewed:
#     content_subscription_created_at, content_subscription_activated_at (epoch)
#   subscription_renewed / subscription_cancelled:
#     id, content_customer_id, content_subscription_id
#   Segment checkout tables (javascript_upentertainment_checkout schema):
#     Checkout start = pages where LOWER(path) LIKE '%welcome/payment_condensed%'
#       (the checkout payment page; id, anonymous_id, context_ip, received_at).
#     subscribe_card_error is confirmed (id, anonymous_id, ip_address,
#       error_message, received_at).
#     order_completed is still an ASSUMED table and returned no matching orders
#       in testing. Point that branch at the event or page that marks a
#       completed order before trusting any "Who Ordered" or success-rate field.
#     subscribe_card_error also supplies ip_address and error_message (populated
#     from about 2026-10-25; earlier card errors show "No error message").
#
# No top-level WITH (nested subqueries only). Full rebuild daily after 6am ET;
# invoice-level windows need each invoice's full attempt history.
# =============================================================================

view: payment_gateway {
  derived_table: {
    sql:
      SELECT
        ev.*,
        COALESCE(bk.has_stripe_backup, 0) AS has_stripe_backup,

      CASE WHEN ev.is_payment_event = 1 THEN
      ROW_NUMBER() OVER (PARTITION BY ev.attempt_group ORDER BY ev.event_ts, ev.event_id)
      END AS attempt_number,

      CASE WHEN ev.is_payment_event = 1 THEN
      COUNT(*) OVER (PARTITION BY ev.attempt_group)
      END AS invoice_attempt_count,

      CASE WHEN ev.is_payment_event = 1 THEN
      MAX(CASE WHEN ev.event_type = 'payment_failed' THEN 1 ELSE 0 END)
      OVER (PARTITION BY ev.attempt_group)
      END AS invoice_had_failure,

      CASE WHEN ev.is_payment_event = 1 THEN
      MAX(CASE WHEN ev.event_type = 'payment_succeeded' THEN 1 ELSE 0 END)
      OVER (PARTITION BY ev.attempt_group)
      END AS invoice_paid,

      CASE WHEN ev.is_payment_event = 1 THEN
      MIN(CASE WHEN ev.event_type = 'payment_failed' THEN ev.event_ts END)
      OVER (PARTITION BY ev.attempt_group)
      END AS invoice_first_failure_ts,

      CASE WHEN ev.is_payment_event = 1 THEN
      MIN(CASE WHEN ev.event_type = 'payment_succeeded' THEN ev.event_ts END)
      OVER (PARTITION BY ev.attempt_group)
      END AS invoice_first_success_ts,

      CASE WHEN ev.is_payment_event = 1 THEN
      MAX(CASE WHEN ev.event_type = 'payment_succeeded' THEN ev.gateway_raw END)
      OVER (PARTITION BY ev.attempt_group)
      END AS recovered_via_gateway_raw,

      -- checkout: first order / card error at or after this event, same anonymous_id
      CASE WHEN ev.event_type IN ('checkout_started', 'checkout_card_error') THEN
      MIN(CASE WHEN ev.event_type = 'order_completed' THEN ev.event_ts END)
      OVER (PARTITION BY COALESCE(ev.anonymous_id, ev.event_id) ORDER BY ev.event_ts
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
      END AS next_order_ts,

      CASE WHEN ev.event_type = 'checkout_started' THEN
      MIN(CASE WHEN ev.event_type = 'checkout_card_error' THEN ev.event_ts END)
      OVER (PARTITION BY COALESCE(ev.anonymous_id, ev.event_id) ORDER BY ev.event_ts
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
      END AS next_card_error_ts

      FROM (
      SELECT
      u.*,
      CASE WHEN u.event_type IN ('payment_succeeded', 'payment_failed') THEN 1 ELSE 0 END
      AS is_payment_event,
      CASE WHEN u.event_type IN ('payment_succeeded', 'payment_failed')
      THEN COALESCE(u.invoice_id, u.subscription_id)
      ELSE u.event_id
      END AS attempt_group
      FROM (

      -- ---------------- payment_succeeded ----------------
      SELECT
      ('ps-' || id)::VARCHAR(160)                                   AS event_id,
      'payment_succeeded'::VARCHAR(30)                              AS event_type,
      content_customer_id::VARCHAR(128)                             AS customer_id,
      content_subscription_id::VARCHAR(128)                         AS subscription_id,
      content_invoice_id::VARCHAR(128)                              AS invoice_id,
      "timestamp"                                                     AS event_ts,
      DATE(CONVERT_TIMEZONE('America/New_York', "timestamp"))         AS report_date,
      LOWER(COALESCE(content_transaction_gateway, content_card_gateway))::VARCHAR(64)
      AS gateway_raw,
      (CASE content_subscription_billing_period_unit
      WHEN 'month' THEN 'monthly' WHEN 'year' THEN 'yearly' ELSE 'unknown'
      END)::VARCHAR(10)                                            AS billing_period,
      (CASE WHEN DATEDIFF(day,
      TIMESTAMP 'epoch' + content_subscription_activated_at * INTERVAL '1 second',
      "timestamp") = 0
      THEN 'new' ELSE 'renewal' END)::VARCHAR(10)             AS customer_type,
      NULL::VARCHAR(10)                                             AS churn_cohort,
      NULL::VARCHAR(128)                                            AS error_code,
      NULL::VARCHAR(1024)                                           AS error_text,
      NULL::VARCHAR(128)                                            AS cancel_reason,
      content_transaction_amount::BIGINT                            AS amount_cents,
      content_transaction_currency_code::VARCHAR(3)                 AS currency_code,
      LOWER(content_card_card_type)::VARCHAR(32)                    AS card_type,
      LOWER(content_card_funding_type)::VARCHAR(32)                 AS card_funding_type,
      0                                                             AS is_dunning_entry,
      TIMESTAMP 'epoch' + content_subscription_created_at * INTERVAL '1 second'  AS subscription_created_at,
      TIMESTAMP 'epoch' + content_subscription_activated_at * INTERVAL '1 second'  AS subscription_activated_at,
      NULL::VARCHAR(128)                                            AS anonymous_id,
      NULL::VARCHAR(64)                                             AS ip_address,
      NULL::VARCHAR(1024)                                           AS checkout_error_message
      FROM chargebee_webhook_events.payment_succeeded
      WHERE content_subscription_subscription_items_0_item_price_id LIKE '%UP%'

      UNION ALL

      -- ---------------- payment_failed ----------------
      SELECT
      ('pf-' || id)::VARCHAR(160),
      'payment_failed'::VARCHAR(30),
      content_customer_id::VARCHAR(128),
      content_subscription_id::VARCHAR(128),
      content_invoice_id::VARCHAR(128),
      "timestamp",
      DATE(CONVERT_TIMEZONE('America/New_York', "timestamp")),
      LOWER(COALESCE(content_transaction_gateway, content_card_gateway))::VARCHAR(64),
      (CASE content_subscription_billing_period_unit
      WHEN 'month' THEN 'monthly' WHEN 'year' THEN 'yearly' ELSE 'unknown'
      END)::VARCHAR(10),
      (CASE WHEN DATEDIFF(day,
      TIMESTAMP 'epoch' + content_subscription_activated_at * INTERVAL '1 second',
      "timestamp") = 0
      THEN 'new' ELSE 'renewal' END)::VARCHAR(10),
      NULL::VARCHAR(10),
      content_transaction_error_code::VARCHAR(128),
      content_transaction_error_text::VARCHAR(1024),
      NULL::VARCHAR(128),
      content_transaction_amount::BIGINT,
      content_transaction_currency_code::VARCHAR(3),
      LOWER(content_card_card_type)::VARCHAR(32),
      LOWER(content_card_funding_type)::VARCHAR(32),
      -- same "new dunning" rule as the existing dunning tile
      CASE WHEN content_invoice_linked_payments_1_txn_date IS NULL THEN 1 ELSE 0 END,
      TIMESTAMP 'epoch' + content_subscription_created_at * INTERVAL '1 second',
      TIMESTAMP 'epoch' + content_subscription_activated_at * INTERVAL '1 second',
      NULL::VARCHAR(128),
      NULL::VARCHAR(64),
      NULL::VARCHAR(1024)
      FROM chargebee_webhook_events.payment_failed
      WHERE content_subscription_subscription_items_0_item_price_id LIKE '%UP%'

      UNION ALL

      -- ---------------- subscription_renewed ----------------
      SELECT
      ('sr-' || id)::VARCHAR(160),
      'subscription_renewed'::VARCHAR(30),
      content_customer_id::VARCHAR(128),
      content_subscription_id::VARCHAR(128),
      NULL::VARCHAR(128),
      "timestamp",
      DATE(CONVERT_TIMEZONE('America/New_York', "timestamp")),
      LOWER(content_customer_payment_method_gateway)::VARCHAR(64),
      (CASE content_subscription_billing_period_unit
      WHEN 'month' THEN 'monthly' WHEN 'year' THEN 'yearly' ELSE 'unknown'
      END)::VARCHAR(10),
      'renewal'::VARCHAR(10),
      NULL::VARCHAR(10),
      NULL::VARCHAR(128),
      NULL::VARCHAR(1024),
      NULL::VARCHAR(128),
      NULL::BIGINT,
      NULL::VARCHAR(3),
      NULL::VARCHAR(32),
      NULL::VARCHAR(32),
      0,
      TIMESTAMP 'epoch' + content_subscription_created_at * INTERVAL '1 second',
      TIMESTAMP 'epoch' + content_subscription_activated_at * INTERVAL '1 second',
      NULL::VARCHAR(128),
      NULL::VARCHAR(64),
      NULL::VARCHAR(1024)
      FROM chargebee_webhook_events.subscription_renewed
      -- same product filter as payment events so the dunning rate's
      -- numerator and denominator cover the same subscriptions
      WHERE content_subscription_subscription_items_0_item_price_id LIKE '%UP%'

      UNION ALL

      -- ---------------- involuntary churn (+ last error within 15 days) ----------------
      SELECT
      ('sc-' || j.id)::VARCHAR(160),
      'involuntary_churn'::VARCHAR(30),
      j.customer_id,
      j.subscription_id,
      NULL::VARCHAR(128),
      j.event_ts,
      j.report_date,
      j.gateway_raw,
      j.billing_period,
      (CASE WHEN j.churn_cohort = 'trial' THEN 'new' ELSE 'renewal' END)::VARCHAR(10),
      j.churn_cohort,
      j.last_error_code,
      j.last_error_text,
      j.cancel_reason,
      NULL::BIGINT,
      NULL::VARCHAR(3),
      NULL::VARCHAR(32),
      NULL::VARCHAR(32),
      0,
      j.subscription_created_at,
      j.subscription_activated_at,
      NULL::VARCHAR(128),
      NULL::VARCHAR(64),
      NULL::VARCHAR(1024)
      FROM (
      SELECT
      c.*,
      p.error_code::VARCHAR(128)  AS last_error_code,
      p.error_text::VARCHAR(1024) AS last_error_text,
      ROW_NUMBER() OVER (
      PARTITION BY c.customer_id, c.report_date
      ORDER BY p.failed_date DESC, p.failed_ts DESC
      ) AS rn
      FROM (
      SELECT
      id,
      content_customer_id::VARCHAR(128)     AS customer_id,
      content_subscription_id::VARCHAR(128) AS subscription_id,
      "timestamp"                             AS event_ts,
      DATE(DATEADD(hour, 18, "timestamp"))    AS report_date,   -- existing churn rule
      (CASE WHEN content_card_gateway IS NULL THEN 'paypal'
      ELSE LOWER(content_card_gateway) END)::VARCHAR(64) AS gateway_raw,
      (CASE content_subscription_billing_period_unit
      WHEN 'month' THEN 'monthly' WHEN 'year' THEN 'yearly' ELSE 'unknown'
      END)::VARCHAR(10)                    AS billing_period,
      -- 'trial' only exists for subscriptions created before the 2026-09-09
      -- switch to paid-only (1788926400 = 2026-09-09 00:00 ET)
      (CASE WHEN content_subscription_created_at < 1788926400
      AND content_subscription_cancelled_at - content_subscription_created_at < 2000800
      THEN 'trial' ELSE 'renew' END)::VARCHAR(10) AS churn_cohort,
      content_subscription_cancel_reason_code::VARCHAR(128) AS cancel_reason,
      TIMESTAMP 'epoch' + content_subscription_created_at * INTERVAL '1 second' AS subscription_created_at,
      TIMESTAMP 'epoch' + content_subscription_activated_at * INTERVAL '1 second' AS subscription_activated_at
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE content_subscription_cancel_reason_code IN (
      'Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer',
      'Tax Calculation Failed', 'Currency incompatible with Gateway',
      'Non Compliant Customer')
      AND content_subscription_cancelled_at - content_subscription_activated_at > 10000
      AND content_subscription_subscription_items LIKE '%UP%'
      ) c
      LEFT JOIN (
      SELECT
      content_customer_id::VARCHAR(128)  AS customer_id,
      "timestamp"                          AS failed_ts,
      DATE(DATEADD(hour, -5, "timestamp")) AS failed_date,
      content_transaction_error_code     AS error_code,
      content_transaction_error_text     AS error_text
      FROM chargebee_webhook_events.payment_failed
      ) p
      ON  p.customer_id = c.customer_id
      AND p.failed_date >= DATEADD(day, -15, c.report_date)
      AND p.failed_date <  c.report_date
      ) j
      WHERE j.rn = 1

      UNION ALL

      -- ---------------- checkout: checkout_started = payment page view ----------------
      -- First view of the checkout payment page per visitor per ET day, so page
      -- reloads (including the reload after a card error) count once.
      SELECT
      ('ck-' || id)::VARCHAR(160),
      'checkout_started'::VARCHAR(30),
      NULL::VARCHAR(128), NULL::VARCHAR(128), NULL::VARCHAR(128),
      received_at,
      DATE(CONVERT_TIMEZONE('America/New_York', received_at)),
      NULL::VARCHAR(64), NULL::VARCHAR(10), NULL::VARCHAR(10), NULL::VARCHAR(10),
      NULL::VARCHAR(128), NULL::VARCHAR(1024), NULL::VARCHAR(128),
      NULL::BIGINT, NULL::VARCHAR(3), NULL::VARCHAR(32), NULL::VARCHAR(32),
      0,
      NULL::TIMESTAMP, NULL::TIMESTAMP,
      anonymous_id::VARCHAR(128),
      context_ip::VARCHAR(64),
      NULL::VARCHAR(1024)
      FROM (
      SELECT
      id, anonymous_id, context_ip, received_at,
      ROW_NUMBER() OVER (
      PARTITION BY anonymous_id, DATE(CONVERT_TIMEZONE('America/New_York', received_at))
      ORDER BY received_at, id
      ) AS rn
      FROM javascript_upentertainment_checkout.pages
      WHERE anonymous_id IS NOT NULL
      AND LOWER(path) LIKE '%welcome/payment_condensed%'
      ) pv
      WHERE pv.rn = 1

      UNION ALL

      -- ---------------- checkout: checkout_card_error (Segment, keyed by anonymous_id) ----------------
      SELECT
      ('ce-' || id)::VARCHAR(160),
      'checkout_card_error'::VARCHAR(30),
      NULL::VARCHAR(128), NULL::VARCHAR(128), NULL::VARCHAR(128),
      received_at,
      DATE(CONVERT_TIMEZONE('America/New_York', received_at)),
      NULL::VARCHAR(64), NULL::VARCHAR(10), NULL::VARCHAR(10), NULL::VARCHAR(10),
      NULL::VARCHAR(128), NULL::VARCHAR(1024), NULL::VARCHAR(128),
      NULL::BIGINT, NULL::VARCHAR(3), NULL::VARCHAR(32), NULL::VARCHAR(32),
      0,
      NULL::TIMESTAMP, NULL::TIMESTAMP,
      anonymous_id::VARCHAR(128),
      ip_address::VARCHAR(64),
      error_message::VARCHAR(1024)
      FROM javascript_upentertainment_checkout.subscribe_card_error
      WHERE anonymous_id IS NOT NULL

      UNION ALL

      -- ---------------- checkout: order_completed (Segment, keyed by anonymous_id) ----------------
      SELECT
      ('oc-' || id)::VARCHAR(160),
      'order_completed'::VARCHAR(30),
      NULL::VARCHAR(128), NULL::VARCHAR(128), NULL::VARCHAR(128),
      received_at,
      DATE(CONVERT_TIMEZONE('America/New_York', received_at)),
      NULL::VARCHAR(64), NULL::VARCHAR(10), NULL::VARCHAR(10), NULL::VARCHAR(10),
      NULL::VARCHAR(128), NULL::VARCHAR(1024), NULL::VARCHAR(128),
      NULL::BIGINT, NULL::VARCHAR(3), NULL::VARCHAR(32), NULL::VARCHAR(32),
      0,
      NULL::TIMESTAMP, NULL::TIMESTAMP,
      anonymous_id::VARCHAR(128),
      NULL::VARCHAR(64),
      NULL::VARCHAR(1024)
      FROM javascript_upentertainment_checkout.order_completed
      WHERE anonymous_id IS NOT NULL

      ) u
      ) ev

      -- Customers with a Stripe source still on file (current state, not as-of event)
      LEFT JOIN (
      SELECT a.content_customer_id::VARCHAR(128) AS customer_id, 1 AS has_stripe_backup
      FROM chargebee_webhook_events.payment_source_added a
      WHERE a.content_payment_source_gateway = 'stripe'
      AND NOT EXISTS (
      SELECT 1
      FROM chargebee_webhook_events.payment_source_deleted d
      WHERE d.content_customer_id = a.content_customer_id
      AND d.content_payment_source_gateway = 'stripe'
      AND d.received_at > a.received_at
      )
      GROUP BY 1
      ) bk
      ON bk.customer_id = ev.customer_id
      ;;

    sql_trigger_value: SELECT TRUNC(DATEADD(hour, -6, CONVERT_TIMEZONE('America/New_York', GETDATE()))) ;;
    distribution: "customer_id"
    sortkeys: ["report_date"]
  }

  # ===========================================================================
  # PERIOD OVER PERIOD  (dashboard only - excluded from the CA explore)
  # Two required date filters, set independently:
  #   Current Period  -> pop_date_range        e.g. is in the last 7 complete days
  #   Prior Period    -> pop_prior_date_range  e.g. is 14 days ago for 7 days
  # Use complete days so a partial "today" isn't compared with a full day.
  # ===========================================================================

  filter: pop_date_range {
    view_label: "_Period over Period"
    label: "Current Period"
    type: date
    description: "Dashboard control. Date range of the current period, e.g. 'is in the last 7 complete days'. Required by every Period-over-Period field."
  }

  filter: pop_prior_date_range {
    view_label: "_Period over Period"
    label: "Prior Period"
    type: date
    description: "Dashboard control. Date range to compare against, e.g. 'is 14 days ago for 7 days'. Any range works; keep it the same length as the Current Period for a like-for-like comparison. Ranges that span 2026-09-09 mix free-trial and paid-only signups; filter Signup Model to keep them like-for-like."
  }

  dimension: pop_curr_start  { hidden: yes type: string sql: {% date_start pop_date_range %} ;; }
  dimension: pop_curr_end    { hidden: yes type: string sql: {% date_end pop_date_range %} ;; }
  dimension: pop_prior_start { hidden: yes type: string sql: {% date_start pop_prior_date_range %} ;; }
  dimension: pop_prior_end   { hidden: yes type: string sql: {% date_end pop_prior_date_range %} ;; }

  dimension: pop_period {
    view_label: "_Period over Period"
    label: "Period"
    type: string
    description: "Dashboard only. 'This Period' or 'Prior Period' based on the Current Period and Prior Period filters; null when outside both. If the two ranges overlap, overlapping days count as This Period."
    sql:
      CASE
        WHEN ${TABLE}.report_date >= ${pop_curr_start}  AND ${TABLE}.report_date < ${pop_curr_end}  THEN 'This Period'
        WHEN ${TABLE}.report_date >= ${pop_prior_start} AND ${TABLE}.report_date < ${pop_prior_end} THEN 'Prior Period'
      END ;;
  }

  dimension: pop_day_index {
    view_label: "_Period over Period"
    label: "Day of Period"
    type: number
    description: "Dashboard only. 1, 2, 3... within each period. Use as the x-axis to overlay This vs Prior Period."
    sql:
      CASE
        WHEN ${pop_period} = 'This Period'  THEN DATEDIFF(day, ${pop_curr_start},  ${TABLE}.report_date) + 1
        WHEN ${pop_period} = 'Prior Period' THEN DATEDIFF(day, ${pop_prior_start}, ${TABLE}.report_date) + 1
      END ;;
  }

  # ===========================================================================
  # DIMENSIONS - identifiers (hidden from browsing; drill only)
  # ===========================================================================

  dimension: event_id {
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.event_id ;;
  }

  dimension: customer_id {
    view_label: "Payment Gateway - Record Detail"
    label: "Customer ID"
    type: string
    description: "Chargebee customer ID. Record-level drill only; do not list unless the user asks for specific customers. For counts use the Customer measures."
    sql: ${TABLE}.customer_id ;;
  }

  dimension: subscription_id {
    view_label: "Payment Gateway - Record Detail"
    label: "Subscription ID"
    type: string
    description: "Chargebee subscription ID. Record-level drill only."
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: invoice_id {
    view_label: "Payment Gateway - Record Detail"
    label: "Invoice ID"
    type: string
    description: "Chargebee invoice ID for payment attempts. Null for renewals and churn. Record-level drill only."
    sql: ${TABLE}.invoice_id ;;
  }

  # ===========================================================================
  # DIMENSIONS - when
  # ===========================================================================

  dimension_group: report {
    group_label: "Report Date"
    label: "Report"
    type: time
    datatype: date
    convert_tz: no
    timeframes: [raw, date, week, month, quarter, year, day_of_week, day_of_month]
    description: "Default date for every question ('yesterday', 'last week', 'in September'). Eastern-time date of the event; invoices are dated by their first attempt; involuntary churn uses the existing churn reporting-date rule. Also called: date, day, week, month."
    sql: ${TABLE}.report_date ;;
  }

  dimension_group: event_et {
    group_label: "Event Time (ET)"
    label: "Event"
    type: time
    convert_tz: no
    timeframes: [raw, time, hour_of_day, day_of_week]
    description: "Exact Eastern-time timestamp of the event. Use Hour of Day / Day of Week to study when charges succeed or fail (retry timing). For date questions use Report Date."
    sql: CONVERT_TIMEZONE('America/New_York', ${TABLE}.event_ts) ;;
  }

  # ===========================================================================
  # DIMENSIONS - who / where (the dashboard filters)
  # ===========================================================================

  dimension: event_type {
    label: "Event Type"
    type: string
    description: "What happened. Values: payment_succeeded (a charge was approved), payment_failed (a charge was declined), subscription_renewed (a subscription renewed and was paid), involuntary_churn (a subscription was cancelled for non-payment or a payment-related reason), checkout_started (a web visitor viewed the checkout payment page; first view per visitor per day), checkout_card_error (a card error at checkout), order_completed (a web order was completed). Prefer the named measures over filtering on this field."
    suggestions: ["payment_succeeded", "payment_failed", "subscription_renewed", "involuntary_churn", "checkout_started", "checkout_card_error", "order_completed"]
    sql: ${TABLE}.event_type ;;
  }

  dimension: gateway {
    label: "Gateway"
    type: string
    description: "Payment processor that handled the charge. Values: braintree, paypal, stripe (unknown if missing). Also called: processor, payment provider, payment method gateway."
    suggestions: ["braintree", "paypal", "stripe"]
    sql:
      CASE
        WHEN ${TABLE}.gateway_raw LIKE '%paypal%'    THEN 'paypal'
        WHEN ${TABLE}.gateway_raw LIKE '%braintree%' THEN 'braintree'
        WHEN ${TABLE}.gateway_raw LIKE '%stripe%'    THEN 'stripe'
        WHEN ${TABLE}.gateway_raw IS NULL            THEN 'unknown'
        ELSE ${TABLE}.gateway_raw
      END ;;
  }

  dimension: billing_period {
    label: "Billing Period"
    type: string
    description: "Subscription billing frequency. Values: monthly, yearly (unknown if missing). Also called: plan term, billing frequency; 'annual' means yearly."
    suggestions: ["monthly", "yearly"]
    sql: ${TABLE}.billing_period ;;
  }

  dimension: customer_type {
    label: "Customer Type"
    type: string
    description: "Values: new = first paid charge on the activation day (a free-trial conversion for subscriptions created before 2026-09-09; the signup purchase for paid-only subscriptions created on or after 2026-09-09); renewal = recurring charge. Pair with Signup Model; for the paid-only first rebill use Lifecycle Stage = First renewal. On involuntary churn rows, new = churned from the trial cohort. Also called: new vs existing, first charge."
    suggestions: ["new", "renewal"]
    sql: ${TABLE}.customer_type ;;
  }

  dimension: churn_cohort {
    label: "Churn Cohort"
    type: string
    description: "Involuntary churn rows only. Values: trial (free-trial subscriptions created before 2026-09-09 and cancelled within ~23 days of creation), renew (everything else, including all paid-only subscriptions). The trial cohort winds down to zero after the 9/9/2026 switch; use Lifecycle Stage for paid-only early churn. Null for payment and renewal events."
    suggestions: ["trial", "renew"]
    sql: ${TABLE}.churn_cohort ;;
  }

  dimension: cancel_reason {
    label: "Cancel Reason"
    type: string
    description: "Involuntary churn rows only. Chargebee cancel reason code, e.g. Not Paid, No Card, Fraud Review Failed, Tax Calculation Failed, Currency incompatible with Gateway."
    sql: ${TABLE}.cancel_reason ;;
  }

  dimension: card_type {
    label: "Card Brand"
    type: string
    description: "Card network on card payments, e.g. visa, mastercard, american_express, discover. Null for PayPal and non-payment events. Also called: card network, card type."
    sql: ${TABLE}.card_type ;;
  }

  dimension: card_funding_type {
    label: "Card Funding Type"
    type: string
    description: "Values: credit, debit, prepaid. Null for PayPal and non-payment events. Prepaid cards commonly fail on trial conversion."
    suggestions: ["credit", "debit", "prepaid"]
    sql: ${TABLE}.card_funding_type ;;
  }

  dimension: has_stripe_backup {
    label: "Has Stripe Backup Card"
    type: yesno
    description: "Yes if the customer currently has a Stripe payment source on file (current state, not as of the event). Use to evaluate Braintree-to-Stripe failover. Also called: backup payment method, secondary card."
    sql: ${TABLE}.has_stripe_backup = 1 ;;
  }

  # ===========================================================================
  # DIMENSIONS - signup model (free trial until 2026-09-08, paid-only from 2026-09-09)
  # ===========================================================================

  dimension_group: subscription_created {
    group_label: "Signup Cohort"
    label: "Subscription Created"
    type: time
    convert_tz: no
    timeframes: [raw, date, week, month]
    description: "Eastern-time date the subscription was created (signup date)."
    sql: CONVERT_TIMEZONE('America/New_York', ${TABLE}.subscription_created_at) ;;
  }

  dimension: signup_cohort {
    group_label: "Signup Cohort"
    label: "Signup Model"
    type: string
    description: "How the subscriber signed up. Values: free_trial = created before 2026-09-09 (free-trial experience); paid_only = created on or after 2026-09-09 (paid at signup, no trial). Use to separate legacy trial subscribers from paid-only subscribers; compare new-customer metrics within one model, not across the 9/9/2026 switch. Also called: trial vs no-trial, acquisition model, cohort."
    suggestions: ["free_trial", "paid_only"]
    sql:
      CASE
        WHEN ${TABLE}.subscription_created_at IS NULL THEN 'unknown'
        WHEN CONVERT_TIMEZONE('America/New_York', ${TABLE}.subscription_created_at) < '2026-09-09' THEN 'free_trial'
        ELSE 'paid_only'
      END ;;
  }

  dimension: days_since_activation {
    group_label: "Signup Cohort"
    label: "Days Since Activation"
    type: number
    description: "Days between the subscription's first paid activation (trial conversion for free_trial, signup for paid_only) and this event."
    sql: DATEDIFF(day, ${TABLE}.subscription_activated_at, ${TABLE}.event_ts) ;;
  }

  dimension: renewal_stage {
    group_label: "Signup Cohort"
    label: "Lifecycle Stage"
    type: string
    description: "Where in the subscription lifecycle the event happened. Values: Initial charge (first paid charge: trial conversion before 9/9/2026, signup purchase after); First renewal (first recurring charge after activation, the paid-only equivalent of trial conversion and the highest-risk renewal); Later renewal. Also called: billing cycle, first rebill, month-2 renewal."
    suggestions: ["Initial charge", "First renewal", "Later renewal"]
    sql:
      CASE
        WHEN ${TABLE}.customer_type = 'new' THEN 'Initial charge'
        WHEN ${TABLE}.subscription_activated_at IS NULL THEN NULL
        WHEN ${days_since_activation} < CASE WHEN ${TABLE}.billing_period = 'yearly' THEN 400 ELSE 45 END
          THEN 'First renewal'
        ELSE 'Later renewal'
      END ;;
  }

  # ===========================================================================
  # DIMENSIONS - why it failed
  # ===========================================================================

  dimension: error_code {
    group_label: "Decline Detail"
    label: "Decline Code"
    type: string
    description: "Raw processor decline code. High cardinality; prefer Decline Category. On churn rows, the code of the last failure within 15 days before cancellation."
    sql: ${TABLE}.error_code ;;
  }

  dimension: error_text {
    group_label: "Decline Detail"
    label: "Decline Message"
    type: string
    description: "Raw processor decline message, e.g. 'Insufficient Funds', 'Closed Card'. High cardinality; use Decline Category for summaries and only use this when the user asks for specific messages. On churn rows, the last failure within 15 days before cancellation. Also called: error message, decline reason."
    sql: ${TABLE}.error_text ;;
  }

  dimension: error_category {
    group_label: "Decline Detail"
    label: "Decline Category"
    type: string
    description: "Decline messages grouped by what can be done about them. Values: Soft decline - insufficient funds; Hard decline - card unusable (closed, expired, lost/stolen, no account, lifecycle); Fraud / risk; PayPal agreement / instrument; Issuer refusal (do not honor, call issuer, policy, restriction); Processor decline (generic); Other. Use this first for any 'why are payments failing' question. Also called: decline type, failure reason, root cause."
    suggestions: ["Soft decline - insufficient funds", "Hard decline - card unusable", "Fraud / risk", "PayPal agreement / instrument", "Issuer refusal", "Processor decline (generic)", "Other"]
    sql:
      CASE
        WHEN ${TABLE}.error_text IS NULL THEN NULL
        WHEN LOWER(${TABLE}.error_text) LIKE '%insufficient funds%'   THEN 'Soft decline - insufficient funds'
        WHEN LOWER(${TABLE}.error_text) LIKE '%life cycle%'
          OR LOWER(${TABLE}.error_text) LIKE '%closed card%'
          OR LOWER(${TABLE}.error_text) LIKE '%no account%'
          OR LOWER(${TABLE}.error_text) LIKE '%expired%'
          OR LOWER(${TABLE}.error_text) LIKE '%pick up%'
          OR LOWER(${TABLE}.error_text) LIKE '%lost%'
          OR LOWER(${TABLE}.error_text) LIKE '%stolen%'
          OR LOWER(${TABLE}.error_text) LIKE '%not activated%'        THEN 'Hard decline - card unusable'
        WHEN LOWER(${TABLE}.error_text) LIKE '%fraud%'                THEN 'Fraud / risk'
        WHEN LOWER(${TABLE}.error_text) LIKE '%agreement%'
          OR LOWER(${TABLE}.error_text) LIKE '%instrument%'
          OR LOWER(${TABLE}.error_text) LIKE '%request was refused%'  THEN 'PayPal agreement / instrument'
        WHEN LOWER(${TABLE}.error_text) LIKE '%policy%'
          OR LOWER(${TABLE}.error_text) LIKE '%restriction%'
          OR LOWER(${TABLE}.error_text) LIKE '%do not honor%'
          OR LOWER(${TABLE}.error_text) LIKE '%call issuer%'          THEN 'Issuer refusal'
        WHEN LOWER(${TABLE}.error_text) LIKE '%processor declined%'   THEN 'Processor decline (generic)'
        ELSE 'Other'
      END ;;
  }

  dimension: suggested_action {
    group_label: "Decline Detail"
    label: "Suggested Action"
    type: string
    description: "Default remediation playbook for each Decline Category. Use when the user asks what to do about a decline type. Edit to match the team's retry and outreach setup."
    sql:
      CASE ${error_category}
        WHEN 'Soft decline - insufficient funds' THEN 'Retry timing (paydays: 1st / 15th)'
        WHEN 'Hard decline - card unusable'      THEN 'Card-update outreach / account updater; stop retrying same card'
        WHEN 'Fraud / risk'                      THEN 'Review fraud and AVS/CVV rules'
        WHEN 'PayPal agreement / instrument'     THEN 'Ask customer to re-authorize PayPal'
        WHEN 'Issuer refusal'                    THEN 'Retry on alternate gateway'
        WHEN 'Processor decline (generic)'       THEN 'Retry on alternate gateway'
      END ;;
  }

  # ===========================================================================
  # DIMENSIONS - web checkout
  # ===========================================================================

  dimension: anonymous_id {
    view_label: "Payment Gateway - Record Detail"
    label: "Checkout Visitor ID"
    type: string
    description: "Segment anonymous_id for web checkout events. Record-level drill only."
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: checkout_converted {
    group_label: "Checkout"
    label: "Checkout Converted"
    type: yesno
    description: "Checkout started or card error rows: yes if the same visitor completed an order within 2 days after this event."
    sql: ${TABLE}.next_order_ts IS NOT NULL AND ${TABLE}.next_order_ts <= DATEADD(day, 2, ${TABLE}.event_ts) ;;
  }

  dimension: checkout_had_card_error {
    group_label: "Checkout"
    label: "Checkout Had Card Error"
    type: yesno
    description: "Payment page rows: yes if the visitor got a card error within 2 days after their first payment page view that day."
    sql: ${TABLE}.next_card_error_ts IS NOT NULL AND ${TABLE}.next_card_error_ts <= DATEADD(day, 2, ${TABLE}.event_ts) ;;
  }

  dimension: checkout_card_error_group {
    group_label: "Checkout"
    label: "Checkout Card Error Group"
    type: string
    description: "Checkout started rows only. Values: Had card error, No card error. Use with Checkout Success Rate to compare the two groups."
    suggestions: ["Had card error", "No card error"]
    sql:
      CASE
        WHEN ${TABLE}.event_type <> 'checkout_started' THEN NULL
        WHEN ${checkout_had_card_error} THEN 'Had card error'
        ELSE 'No card error'
      END ;;
  }

  # ===========================================================================
  # DIMENSIONS - checkout card errors (checkout_card_error rows only)
  # ===========================================================================

  dimension: ip_address {
    view_label: "Payment Gateway - Record Detail"
    label: "Checkout IP Address"
    type: string
    description: "IP address on a checkout card error or payment page view. Personal data: record-level investigation only (for example, many errors from one address can signal card testing). Never list in summaries."
    sql: ${TABLE}.ip_address ;;
  }

  dimension: checkout_error_message {
    group_label: "Checkout Card Errors"
    label: "Checkout Error Message"
    type: string
    description: "Raw card error message shown at checkout, e.g. 'Problem while adding the payment method. Error message : (2010) Card Issuer Declined CVV.' High cardinality; prefer Checkout Error Reason or Checkout Error Category."
    sql: ${TABLE}.checkout_error_message ;;
  }

  dimension: checkout_error_code {
    group_label: "Checkout Card Errors"
    label: "Checkout Error Code"
    type: string
    description: "Gateway response code from the checkout error message, e.g. 2010, 2014, 93108. Null for messages without a code, such as prepaid cards."
    sql: REGEXP_SUBSTR(${TABLE}.checkout_error_message, '[(]([0-9]+)[)]', 1, 1, 'e') ;;
  }

  dimension: checkout_error_reason {
    group_label: "Checkout Card Errors"
    label: "Checkout Error Reason"
    type: string
    description: "Checkout error message without the standard prefix and code, e.g. 'Prepaid cards are not supported', 'Card Issuer Declined CVV', 'Processor Declined - Fraud Suspected'. Use for the top individual reasons."
    sql:
      CASE
        WHEN ${TABLE}.event_type <> 'checkout_card_error' THEN NULL
        WHEN ${TABLE}.checkout_error_message IS NULL THEN 'No error message'
        ELSE TRIM(TRAILING '.' FROM TRIM(
               REGEXP_REPLACE(
                 REGEXP_REPLACE(${TABLE}.checkout_error_message, '^.*Error message *: *', ''),
                 '^[(][0-9]+[)] *', '')))
      END ;;
  }

  dimension: checkout_error_category {
    group_label: "Checkout Card Errors"
    label: "Checkout Error Category"
    type: string
    description: "Checkout card errors grouped by cause. Values: Prepaid card not accepted; CVV mismatch; Fraud / risk rules; Checkout technical error (expired or reused payment token); Card unusable; Issuer refusal; Insufficient funds; Processor decline; Other; No error message (before message capture began about 2026-10-25). Use this first for 'what are the top checkout errors' questions. Also called: signup decline reason, checkout error type."
    suggestions: ["Prepaid card not accepted", "CVV mismatch", "Fraud / risk rules", "Checkout technical error", "Card unusable", "Issuer refusal", "Insufficient funds", "Processor decline", "Other", "No error message"]
    sql:
      CASE
        WHEN ${TABLE}.event_type <> 'checkout_card_error' THEN NULL
        WHEN ${TABLE}.checkout_error_message IS NULL THEN 'No error message'
        WHEN LOWER(${TABLE}.checkout_error_message) LIKE '%prepaid%'                 THEN 'Prepaid card not accepted'
        WHEN LOWER(${TABLE}.checkout_error_message) LIKE '%cvv%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%security code%'           THEN 'CVV mismatch'
        WHEN LOWER(${TABLE}.checkout_error_message) LIKE '%nonce%'                   THEN 'Checkout technical error'
        WHEN LOWER(${TABLE}.checkout_error_message) LIKE '%fraud%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%security violation%'      THEN 'Fraud / risk rules'
        WHEN LOWER(${TABLE}.checkout_error_message) LIKE '%insufficient funds%'      THEN 'Insufficient funds'
        WHEN LOWER(${TABLE}.checkout_error_message) LIKE '%life cycle%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%pick up%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%no account%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%closed card%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%expired card%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%lost%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%stolen%'                  THEN 'Card unusable'
        WHEN LOWER(${TABLE}.checkout_error_message) LIKE '%processor declined%'      THEN 'Processor decline'
        WHEN LOWER(${TABLE}.checkout_error_message) LIKE '%restriction%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%do not honor%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%call issuer%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%policy%'
          OR LOWER(${TABLE}.checkout_error_message) LIKE '%declined%'                THEN 'Issuer refusal'
        ELSE 'Other'
      END ;;
  }

  dimension: checkout_error_owner {
    group_label: "Checkout Card Errors"
    label: "Checkout Error Owner"
    type: string
    description: "Who can act on the error. Values: Product policy (prepaid cards), Customer entry (CVV), Engineering (payment token errors), Fraud rules, Card issuer (bank declines). Use to separate errors UP can fix from true bank declines."
    suggestions: ["Product policy", "Customer entry", "Engineering", "Fraud rules", "Card issuer"]
    sql:
      CASE ${checkout_error_category}
        WHEN 'Prepaid card not accepted' THEN 'Product policy'
        WHEN 'CVV mismatch'              THEN 'Customer entry'
        WHEN 'Checkout technical error'  THEN 'Engineering'
        WHEN 'Fraud / risk rules'        THEN 'Fraud rules'
        WHEN 'No error message'          THEN 'Unknown'
        WHEN 'Other'                     THEN 'Unknown'
        ELSE 'Card issuer'
      END ;;
  }

  dimension: checkout_error_action {
    group_label: "Checkout Card Errors"
    label: "Checkout Error Suggested Action"
    type: string
    description: "Default next step for each Checkout Error Category. Edit to match the team's plans."
    sql:
      CASE ${checkout_error_category}
        WHEN 'Prepaid card not accepted' THEN 'Decide whether to accept prepaid cards; if not, say so before card entry'
        WHEN 'CVV mismatch'              THEN 'Clarify the security-code field and keep the form filled for a retry'
        WHEN 'Checkout technical error'  THEN 'Fix expired or reused payment tokens (double submits, page timeouts)'
        WHEN 'Fraud / risk rules'        THEN 'Review fraud-rule settings for false positives'
        WHEN 'Card unusable'             THEN 'Prompt for a different card'
        WHEN 'Issuer refusal'            THEN 'Prompt for a different card or PayPal'
        WHEN 'Insufficient funds'        THEN 'Suggest another card or PayPal'
        WHEN 'Processor decline'         THEN 'Offer PayPal as an alternative'
      END ;;
  }

  # ===========================================================================
  # DIMENSIONS - retries and invoice outcome
  # ===========================================================================

  dimension: attempt_number {
    group_label: "Retries"
    label: "Attempt Number"
    type: number
    description: "1 = initial charge for the invoice, 2 = first retry, etc. Null for renewal and churn rows."
    sql: ${TABLE}.attempt_number ;;
  }

  dimension: is_first_attempt {
    group_label: "Retries"
    label: "Is Initial Attempt"
    type: yesno
    description: "Yes on the first charge attempt of each invoice. Invoice-level measures already apply this; no need to filter on it."
    sql: ${TABLE}.attempt_number = 1 ;;
  }

  dimension: attempt_bucket {
    group_label: "Retries"
    label: "Attempt Bucket"
    type: string
    description: "Values: 1 - Initial, 2 - Retry 1, 3 - Retry 2, 4+ - Retry 3+. Pair with Authorization Rate (All Attempts) to see how much each retry recovers. Also called: retry number, dunning step."
    suggestions: ["1 - Initial", "2 - Retry 1", "3 - Retry 2", "4+ - Retry 3+"]
    sql:
      CASE
        WHEN ${TABLE}.attempt_number = 1 THEN '1 - Initial'
        WHEN ${TABLE}.attempt_number = 2 THEN '2 - Retry 1'
        WHEN ${TABLE}.attempt_number = 3 THEN '3 - Retry 2'
        WHEN ${TABLE}.attempt_number >= 4 THEN '4+ - Retry 3+'
      END ;;
  }

  dimension: invoice_attempt_count {
    group_label: "Retries"
    label: "Attempts on Invoice"
    type: number
    description: "Total charge attempts made on the invoice so far."
    sql: ${TABLE}.invoice_attempt_count ;;
  }

  dimension: invoice_had_failure {
    group_label: "Invoice Outcome"
    label: "Invoice Had a Decline"
    type: yesno
    description: "Yes if any charge attempt on the invoice was declined."
    sql: ${TABLE}.invoice_had_failure = 1 ;;
  }

  dimension: invoice_paid {
    group_label: "Invoice Outcome"
    label: "Invoice Paid"
    type: yesno
    description: "Yes if any charge attempt on the invoice succeeded."
    sql: ${TABLE}.invoice_paid = 1 ;;
  }

  dimension: invoice_status {
    group_label: "Invoice Outcome"
    label: "Invoice Status"
    type: string
    description: "Final outcome of the invoice. Values: Paid on first try; Recovered (declined, then paid on a retry); Still in dunning (unpaid, first decline under 30 days ago); Not recovered (unpaid after 30+ days). Also called: payment outcome, collection status."
    suggestions: ["Paid on first try", "Recovered", "Still in dunning", "Not recovered"]
    sql:
      CASE
        WHEN ${TABLE}.is_payment_event = 0 THEN NULL
        WHEN ${TABLE}.invoice_had_failure = 0 AND ${TABLE}.invoice_paid = 1 THEN 'Paid on first try'
        WHEN ${TABLE}.invoice_had_failure = 1 AND ${TABLE}.invoice_paid = 1 THEN 'Recovered'
        WHEN ${TABLE}.invoice_first_failure_ts >= DATEADD(day, -30, GETDATE()) THEN 'Still in dunning'
        ELSE 'Not recovered'
      END ;;
  }

  dimension: recovered_via_gateway {
    group_label: "Invoice Outcome"
    label: "Recovered via Gateway"
    type: string
    description: "Gateway that finally collected a declined invoice. Values: braintree, paypal, stripe."
    suggestions: ["braintree", "paypal", "stripe"]
    sql:
      CASE
        WHEN ${TABLE}.recovered_via_gateway_raw LIKE '%paypal%'    THEN 'paypal'
        WHEN ${TABLE}.recovered_via_gateway_raw LIKE '%braintree%' THEN 'braintree'
        WHEN ${TABLE}.recovered_via_gateway_raw LIKE '%stripe%'    THEN 'stripe'
        ELSE ${TABLE}.recovered_via_gateway_raw
      END ;;
  }

  dimension: recovered_on_alternate_gateway {
    group_label: "Invoice Outcome"
    label: "Recovered on Alternate Gateway"
    type: yesno
    description: "Yes if the invoice was declined on one gateway and collected on a different one (e.g. Braintree to Stripe failover)."
    sql: ${TABLE}.invoice_had_failure = 1 AND ${TABLE}.invoice_paid = 1
      AND ${recovered_via_gateway} <> ${gateway} ;;
  }

  dimension: hours_to_recover {
    group_label: "Invoice Outcome"
    label: "Hours to Recover"
    type: number
    description: "Hours between the first decline and the successful charge on a recovered invoice."
    sql:
      CASE WHEN ${TABLE}.invoice_first_success_ts > ${TABLE}.invoice_first_failure_ts
           THEN DATEDIFF(hour, ${TABLE}.invoice_first_failure_ts, ${TABLE}.invoice_first_success_ts)
      END ;;
  }

  # ---- hidden plumbing ------------------------------------------------------

  dimension: is_dunning_entry { hidden: yes type: yesno sql: ${TABLE}.is_dunning_entry = 1 ;; }
  dimension: is_payment_event { hidden: yes type: yesno sql: ${TABLE}.is_payment_event = 1 ;; }

  dimension: amount_usd {
    hidden: yes
    type: number
    value_format_name: usd
    description: "Charge amount in dollars (stored in cents). Use the Revenue measures, not this field."
    sql: ${TABLE}.amount_cents / 100.0 ;;
  }

  # ===========================================================================
  # MEASURES - charge attempts (every attempt, including retries)
  # ===========================================================================

  measure: charge_attempts {
    group_label: "Charge Attempts"
    label: "Charge Attempts"
    type: count
    description: "Number of charge attempts, including retries. Also called: transactions, payment attempts."
    filters: [is_payment_event: "yes"]
    drill_fields: [detail*]
  }

  measure: successful_charges {
    group_label: "Charge Attempts"
    label: "Approved Charge Attempts"
    type: count
    description: "Charge attempts that were approved, including retries. Also called: successful payments, approvals."
    filters: [event_type: "payment_succeeded"]
    drill_fields: [detail*]
  }

  measure: failed_charges {
    group_label: "Charge Attempts"
    label: "Failed Charge Attempts"
    type: count
    description: "Charge attempts that were declined, including retries. Also called: declines, failed payments, failures."
    filters: [event_type: "payment_failed"]
    drill_fields: [detail*]
  }

  measure: attempt_auth_rate {
    group_label: "Charge Attempts"
    label: "Authorization Rate (All Attempts)"
    type: number
    value_format_name: percent_2
    description: "Approved Charge Attempts / Charge Attempts, across initial charges and retries. Pair with Attempt Bucket to see retry effectiveness. Ratio: do not sum or average across rows."
    sql: ${successful_charges} / NULLIF(${charge_attempts}, 0)::FLOAT ;;
  }

  # ===========================================================================
  # MEASURES - invoice level (each invoice counted once, on its first attempt)
  # ===========================================================================

  measure: invoices_attempted {
    group_label: "Authorization"
    label: "Invoices Attempted"
    type: count
    description: "Number of invoices with at least one charge attempt, dated by the first attempt. Also called: billing attempts, charges due."
    filters: [is_first_attempt: "yes"]
    drill_fields: [detail*]
  }

  measure: first_attempt_successes {
    group_label: "Authorization"
    label: "Invoices Approved on First Try"
    type: count
    description: "Invoices whose initial charge was approved."
    filters: [is_first_attempt: "yes", event_type: "payment_succeeded"]
  }

  measure: first_attempt_auth_rate {
    group_label: "Authorization"
    label: "First-Attempt Authorization Rate"
    type: number
    value_format_name: percent_2
    description: "Headline KPI. Invoices Approved on First Try / Invoices Attempted. Purest read of card and gateway quality. Also called: auth rate, approval rate, acceptance rate, success rate. Ratio: do not sum or average across rows."
    sql: ${first_attempt_successes} / NULLIF(${invoices_attempted}, 0)::FLOAT ;;
  }

  measure: invoices_paid {
    group_label: "Authorization"
    label: "Invoices Paid"
    type: count
    description: "Invoices paid on the first try or after retries."
    filters: [is_first_attempt: "yes", invoice_paid: "yes"]
  }

  measure: eventual_auth_rate {
    group_label: "Authorization"
    label: "Eventual Authorization Rate"
    type: number
    value_format_name: percent_2
    description: "Invoices Paid / Invoices Attempted, after all retries. Understated for the last ~30 days while invoices are still in retry. Also called: auth rate after retries, collection rate. Ratio: do not sum or average across rows."
    sql: ${invoices_paid} / NULLIF(${invoices_attempted}, 0)::FLOAT ;;
  }

  # ===========================================================================
  # MEASURES - recovery
  # ===========================================================================

  measure: invoices_with_failure {
    group_label: "Recovery"
    label: "Declined Invoices"
    type: count
    description: "Invoices with at least one declined charge. Also called: failed invoices, invoices in dunning."
    filters: [is_first_attempt: "yes", invoice_had_failure: "yes"]
    drill_fields: [detail*]
  }

  measure: invoices_recovered {
    group_label: "Recovery"
    label: "Recovered Invoices"
    type: count
    description: "Declined invoices that were later paid on a retry. Also called: saves, won back, recovered payments."
    filters: [is_first_attempt: "yes", invoice_had_failure: "yes", invoice_paid: "yes"]
    drill_fields: [detail*]
  }

  measure: invoices_still_in_dunning {
    group_label: "Recovery"
    label: "Invoices Still in Dunning"
    type: count
    description: "Declined invoices not yet paid whose first decline was under 30 days ago (still being retried)."
    filters: [is_first_attempt: "yes", invoice_status: "Still in dunning"]
    drill_fields: [detail*]
  }

  measure: invoices_not_recovered {
    group_label: "Recovery"
    label: "Unrecovered Invoices"
    type: count
    description: "Declined invoices still unpaid 30+ days after the first decline. Also called: lost invoices, bad debt."
    filters: [is_first_attempt: "yes", invoice_status: "Not recovered"]
    drill_fields: [detail*]
  }

  measure: recovery_rate {
    group_label: "Recovery"
    label: "Recovery Rate"
    type: number
    value_format_name: percent_2
    description: "Recovered Invoices / Declined Invoices. Understated for the last ~30 days. Also called: dunning recovery rate, save rate. Ratio: do not sum or average across rows."
    sql: ${invoices_recovered} / NULLIF(${invoices_with_failure}, 0)::FLOAT ;;
  }

  measure: invoices_recovered_on_alternate_gateway {
    group_label: "Recovery"
    label: "Invoices Recovered on Alternate Gateway"
    type: count
    description: "Declined invoices collected on a different gateway than the first attempt (failover saves)."
    filters: [is_first_attempt: "yes", recovered_on_alternate_gateway: "yes"]
  }

  measure: avg_attempts_to_recover {
    group_label: "Recovery"
    label: "Average Attempts to Recover"
    type: average
    value_format_name: decimal_1
    description: "Average number of charge attempts on recovered invoices."
    sql: ${invoice_attempt_count} ;;
    filters: [is_first_attempt: "yes", invoice_had_failure: "yes", invoice_paid: "yes"]
  }

  measure: median_hours_to_recover {
    group_label: "Recovery"
    label: "Median Hours to Recover"
    type: median
    value_format_name: decimal_1
    description: "Median hours from first decline to successful charge on recovered invoices. Also called: time to recover."
    sql: ${hours_to_recover} ;;
    filters: [is_first_attempt: "yes"]
  }

  # ===========================================================================
  # MEASURES - revenue (USD)
  # ===========================================================================

  measure: revenue_at_risk {
    group_label: "Revenue"
    label: "Revenue at Risk"
    type: sum
    value_format_name: usd_0
    description: "Dollar value of invoices that were declined at least once. Also called: failed revenue, declined revenue."
    sql: ${amount_usd} ;;
    filters: [is_first_attempt: "yes", invoice_had_failure: "yes"]
  }

  measure: revenue_recovered {
    group_label: "Revenue"
    label: "Revenue Recovered"
    type: sum
    value_format_name: usd_0
    description: "Dollar value of declined invoices later collected. Also called: saved revenue."
    sql: ${amount_usd} ;;
    filters: [is_first_attempt: "yes", invoice_had_failure: "yes", invoice_paid: "yes"]
  }

  measure: revenue_not_recovered {
    group_label: "Revenue"
    label: "Revenue Not Recovered"
    type: sum
    value_format_name: usd_0
    description: "Dollar value of declined invoices still unpaid 30+ days after the first decline. Also called: lost revenue."
    sql: ${amount_usd} ;;
    filters: [is_first_attempt: "yes", invoice_status: "Not recovered"]
  }

  # ===========================================================================
  # MEASURES - web checkout (distinct visitors by anonymous_id)
  # ===========================================================================

  measure: checkout_visitors {
    group_label: "Checkout"
    label: "Checkout Visitors"
    type: count_distinct
    description: "Distinct visitors who viewed the checkout payment page (/welcome/payment_condensed). Page reloads count once per visitor per day. Also called: checkout starts, payment page visitors."
    sql: ${anonymous_id} ;;
    filters: [event_type: "checkout_started"]
  }

  measure: checkout_completers {
    group_label: "Checkout"
    label: "Checkout Visitors Who Ordered"
    type: count_distinct
    description: "Distinct checkout visitors who completed an order within 2 days of reaching checkout."
    sql: ${anonymous_id} ;;
    filters: [event_type: "checkout_started", checkout_converted: "yes"]
  }

  measure: checkout_success_rate {
    group_label: "Checkout"
    label: "Checkout Success Rate"
    type: number
    value_format_name: percent_2
    description: "Checkout Visitors Who Ordered / Checkout Visitors. Group by Checkout Card Error Group to compare visitors with and without a card error. Also called: checkout conversion, signup success rate. Ratio: do not sum or average across rows."
    sql: ${checkout_completers} / NULLIF(${checkout_visitors}, 0)::FLOAT ;;
  }

  measure: card_errors {
    group_label: "Checkout"
    label: "Checkout Card Errors"
    type: count
    description: "Card error events at checkout (expired, declined or other card errors). Also called: signup declines."
    filters: [event_type: "checkout_card_error"]
  }

  measure: card_error_visitors {
    group_label: "Checkout"
    label: "Visitors With a Card Error"
    type: count_distinct
    description: "Distinct visitors who got at least one card error at checkout."
    sql: ${anonymous_id} ;;
    filters: [event_type: "checkout_card_error"]
  }

  measure: card_error_recovered_visitors {
    group_label: "Checkout"
    label: "Card Error Visitors Who Ordered"
    type: count_distinct
    description: "Distinct visitors who completed an order within 2 days after a card error."
    sql: ${anonymous_id} ;;
    filters: [event_type: "checkout_card_error", checkout_converted: "yes"]
  }

  measure: card_error_recovery_rate {
    group_label: "Checkout"
    label: "Card Error Recovery Rate"
    type: number
    value_format_name: percent_2
    description: "Card Error Visitors Who Ordered / Visitors With a Card Error. Also called: checkout save rate. Ratio: do not sum or average across rows."
    sql: ${card_error_recovered_visitors} / NULLIF(${card_error_visitors}, 0)::FLOAT ;;
  }

  measure: orders_completed {
    group_label: "Checkout"
    label: "Completed Orders"
    type: count
    description: "Order completed events on the checkout site. Also called: orders, successful checkouts, completed signups."
    filters: [event_type: "order_completed"]
  }

  measure: checkout_payment_success_rate {
    group_label: "Checkout"
    label: "Checkout Payment Success Rate"
    type: number
    value_format_name: percent_2
    description: "Completed Orders / (Completed Orders + Checkout Card Errors): the share of payment submissions at checkout that ended in an order rather than a card error. Counts events, so it does not depend on matching visitors across events. Also called: payment success rate, order success rate. Ratio: do not sum or average across rows."
    sql: ${orders_completed} / NULLIF(${orders_completed} + ${card_errors}, 0)::FLOAT ;;
  }

  measure: card_error_ip_addresses {
    group_label: "Checkout"
    label: "Card Error IP Addresses"
    type: count_distinct
    description: "Distinct IP addresses with a checkout card error. Far more errors than addresses can signal card testing."
    sql: ${ip_address} ;;
    filters: [event_type: "checkout_card_error"]
  }

  measure: card_errors_per_visitor {
    group_label: "Checkout"
    label: "Card Errors per Visitor"
    type: number
    value_format_name: decimal_2
    description: "Checkout Card Errors / Visitors With a Card Error: how many times visitors retry after an error. Ratio: do not sum or average across rows."
    sql: ${card_errors} / NULLIF(${card_error_visitors}, 0)::FLOAT ;;
  }

  # ===========================================================================
  # MEASURES - renewal success, unique customers
  # ===========================================================================

  measure: renewal_charge_customers {
    group_label: "Renewal Success"
    label: "Renewing Customers Charged"
    type: count_distinct
    description: "Distinct customers with at least one renewal charge attempt (Customer Type = renewal)."
    sql: ${customer_id} ;;
    filters: [is_payment_event: "yes", customer_type: "renewal"]
  }

  measure: renewal_paid_customers {
    group_label: "Renewal Success"
    label: "Renewing Customers Paid"
    type: count_distinct
    description: "Distinct customers with at least one successful renewal charge, on the first try or after a retry."
    sql: ${customer_id} ;;
    filters: [event_type: "payment_succeeded", customer_type: "renewal"]
  }

  measure: renewal_declined_customers {
    group_label: "Renewal Success"
    label: "Renewing Customers Declined"
    type: count_distinct
    description: "Distinct customers with at least one declined renewal charge."
    sql: ${customer_id} ;;
    filters: [event_type: "payment_failed", customer_type: "renewal"]
  }

  measure: renewal_customer_success_rate {
    group_label: "Renewal Success"
    label: "Renewal Success Rate (Unique Customers)"
    type: number
    value_format_name: percent_2
    description: "Renewing Customers Paid / Renewing Customers Charged. Understated for the last ~30 days while retries continue. Also called: renewal success rate, customer renewal rate. Ratio: do not sum or average across rows."
    sql: ${renewal_paid_customers} / NULLIF(${renewal_charge_customers}, 0)::FLOAT ;;
  }

  measure: renewal_customer_first_try_rate {
    group_label: "Renewal Success"
    label: "Renewal First-Try Rate (Unique Customers)"
    type: number
    value_format_name: percent_2
    description: "Share of renewing customers with no declined renewal charge in the period. Ratio: do not sum or average across rows."
    sql: (${renewal_charge_customers} - ${renewal_declined_customers}) / NULLIF(${renewal_charge_customers}, 0)::FLOAT ;;
  }

  # ===========================================================================
  # MEASURES - dunning, renewals, involuntary churn (distinct customers)
  # ===========================================================================

  measure: dunning_users {
    group_label: "Dunning & Churn"
    label: "New Dunning Customers"
    type: count_distinct
    description: "Distinct customers entering dunning (a declined charge with no earlier linked payment on the invoice). Also called: dunning users, past-due customers, customers with payment issues."
    sql: ${customer_id} ;;
    filters: [event_type: "payment_failed", is_dunning_entry: "yes"]
    drill_fields: [detail*]
  }

  measure: renewed_users {
    group_label: "Dunning & Churn"
    label: "Renewed Customers"
    type: count_distinct
    description: "Distinct customers whose subscription renewed and was paid. Also called: paid renewals."
    sql: ${customer_id} ;;
    filters: [event_type: "subscription_renewed"]
  }

  measure: dunning_rate {
    group_label: "Dunning & Churn"
    label: "Dunning Rate"
    type: number
    value_format_name: percent_2
    description: "New Dunning Customers / (New Dunning Customers + Renewed Customers). Same as the failure-rate line on the existing dashboard. Also called: failure rate, renewal failure rate. Ratio: do not sum or average across rows."
    sql: ${dunning_users} / NULLIF(${dunning_users} + ${renewed_users}, 0)::FLOAT ;;
  }

  measure: involuntary_churn_users {
    group_label: "Dunning & Churn"
    label: "Involuntary Churn Customers"
    type: count_distinct
    description: "Distinct customers cancelled for non-payment or a payment-related reason. Also called: involuntary churn, failed-payment churn, churned due to declines."
    sql: ${customer_id} ;;
    filters: [event_type: "involuntary_churn"]
    drill_fields: [detail*]
  }

  measure: involuntary_churn_trial {
    group_label: "Dunning & Churn"
    label: "Involuntary Churn - Trial Cohort"
    type: count_distinct
    description: "Involuntary churn customers in the trial cohort (cancelled within ~23 days of creation)."
    sql: ${customer_id} ;;
    filters: [event_type: "involuntary_churn", churn_cohort: "trial"]
  }

  measure: involuntary_churn_renew {
    group_label: "Dunning & Churn"
    label: "Involuntary Churn - Renewal Cohort"
    type: count_distinct
    description: "Involuntary churn customers past the trial window."
    sql: ${customer_id} ;;
    filters: [event_type: "involuntary_churn", churn_cohort: "renew"]
  }

  # ===========================================================================
  # KPI MEASURES - this vs prior period (dashboard only; need Current Period)
  # ===========================================================================

  measure: fa_attempted_this  { hidden: yes type: count filters: [is_first_attempt: "yes", pop_period: "This Period"] }
  measure: fa_attempted_prior { hidden: yes type: count filters: [is_first_attempt: "yes", pop_period: "Prior Period"] }
  measure: fa_success_this    { hidden: yes type: count filters: [is_first_attempt: "yes", event_type: "payment_succeeded", pop_period: "This Period"] }
  measure: fa_success_prior   { hidden: yes type: count filters: [is_first_attempt: "yes", event_type: "payment_succeeded", pop_period: "Prior Period"] }

  measure: first_attempt_auth_rate_this_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "First-Attempt Auth Rate - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${fa_success_this} / NULLIF(${fa_attempted_this}, 0)::FLOAT ;;
  }

  measure: first_attempt_auth_rate_prior_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "First-Attempt Auth Rate - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${fa_success_prior} / NULLIF(${fa_attempted_prior}, 0)::FLOAT ;;
  }

  measure: first_attempt_auth_rate_change {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "First-Attempt Auth Rate - Change (pts)"
    description: "Dashboard only. This Period minus Prior Period, in percentage points."
    type: number
    value_format: "+0.00%;-0.00%;0.00%"
    sql: ${first_attempt_auth_rate_this_period} - ${first_attempt_auth_rate_prior_period} ;;
  }

  measure: dun_users_this     { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [event_type: "payment_failed", is_dunning_entry: "yes", pop_period: "This Period"] }
  measure: dun_users_prior    { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [event_type: "payment_failed", is_dunning_entry: "yes", pop_period: "Prior Period"] }
  measure: renew_users_this   { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [event_type: "subscription_renewed", pop_period: "This Period"] }
  measure: renew_users_prior  { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [event_type: "subscription_renewed", pop_period: "Prior Period"] }

  measure: dunning_rate_this_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Dunning Rate - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${dun_users_this} / NULLIF(${dun_users_this} + ${renew_users_this}, 0)::FLOAT ;;
  }

  measure: dunning_rate_prior_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Dunning Rate - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${dun_users_prior} / NULLIF(${dun_users_prior} + ${renew_users_prior}, 0)::FLOAT ;;
  }

  measure: dunning_rate_change {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Dunning Rate - Change (pts)"
    description: "Dashboard only. This Period minus Prior Period, in percentage points."
    type: number
    value_format: "+0.00%;-0.00%;0.00%"
    sql: ${dunning_rate_this_period} - ${dunning_rate_prior_period} ;;
  }

  measure: fail_inv_this   { hidden: yes type: count filters: [is_first_attempt: "yes", invoice_had_failure: "yes", pop_period: "This Period"] }
  measure: fail_inv_prior  { hidden: yes type: count filters: [is_first_attempt: "yes", invoice_had_failure: "yes", pop_period: "Prior Period"] }
  measure: rec_inv_this    { hidden: yes type: count filters: [is_first_attempt: "yes", invoice_had_failure: "yes", invoice_paid: "yes", pop_period: "This Period"] }
  measure: rec_inv_prior   { hidden: yes type: count filters: [is_first_attempt: "yes", invoice_had_failure: "yes", invoice_paid: "yes", pop_period: "Prior Period"] }

  measure: recovery_rate_this_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Recovery Rate - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${rec_inv_this} / NULLIF(${fail_inv_this}, 0)::FLOAT ;;
  }

  measure: recovery_rate_prior_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Recovery Rate - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${rec_inv_prior} / NULLIF(${fail_inv_prior}, 0)::FLOAT ;;
  }

  measure: recovery_rate_change {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Recovery Rate - Change (pts)"
    description: "Dashboard only. This Period minus Prior Period, in percentage points."
    type: number
    value_format: "+0.00%;-0.00%;0.00%"
    sql: ${recovery_rate_this_period} - ${recovery_rate_prior_period} ;;
  }

  measure: involuntary_churn_this_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Involuntary Churn - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: count_distinct
    sql: ${customer_id} ;;
    filters: [event_type: "involuntary_churn", pop_period: "This Period"]
  }

  measure: involuntary_churn_prior_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Involuntary Churn - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: count_distinct
    sql: ${customer_id} ;;
    filters: [event_type: "involuntary_churn", pop_period: "Prior Period"]
  }

  measure: involuntary_churn_change_pct {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Involuntary Churn - Change (%)"
    description: "Dashboard only. Percent change vs Prior Period."
    type: number
    value_format: "+0.0%;-0.0%;0.0%"
    sql: (${involuntary_churn_this_period} - ${involuntary_churn_prior_period})
      / NULLIF(${involuntary_churn_prior_period}, 0)::FLOAT ;;
  }

  measure: revenue_not_recovered_this_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Revenue Not Recovered - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: sum
    value_format_name: usd_0
    sql: ${amount_usd} ;;
    filters: [is_first_attempt: "yes", invoice_status: "Not recovered", pop_period: "This Period"]
  }

  measure: revenue_not_recovered_prior_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Revenue Not Recovered - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: sum
    value_format_name: usd_0
    sql: ${amount_usd} ;;
    filters: [is_first_attempt: "yes", invoice_status: "Not recovered", pop_period: "Prior Period"]
  }

  measure: revenue_not_recovered_change_pct {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Revenue Not Recovered - Change (%)"
    description: "Dashboard only. Percent change vs Prior Period."
    type: number
    value_format: "+0.0%;-0.0%;0.0%"
    sql: (${revenue_not_recovered_this_period} - ${revenue_not_recovered_prior_period})
      / NULLIF(${revenue_not_recovered_prior_period}, 0)::FLOAT ;;
  }

  # Revenue at risk
  measure: revenue_at_risk_this_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Revenue at Risk - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: sum
    value_format_name: usd_0
    sql: ${amount_usd} ;;
    filters: [is_first_attempt: "yes", invoice_had_failure: "yes", pop_period: "This Period"]
  }

  measure: revenue_at_risk_prior_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Revenue at Risk - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: sum
    value_format_name: usd_0
    sql: ${amount_usd} ;;
    filters: [is_first_attempt: "yes", invoice_had_failure: "yes", pop_period: "Prior Period"]
  }

  measure: revenue_at_risk_change_pct {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Revenue at Risk - Change (%)"
    description: "Dashboard only. Percent change vs Prior Period."
    type: number
    value_format: "+0.0%;-0.0%;0.0%"
    sql: (${revenue_at_risk_this_period} - ${revenue_at_risk_prior_period})
      / NULLIF(${revenue_at_risk_prior_period}, 0)::FLOAT ;;
  }

  # Invoices attempted (use with Invoice Status for outcome changes)
  measure: invoices_attempted_this_period {
    view_label: "_Period over Period"
    group_label: "Volumes"
    label: "Invoices Attempted - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${fa_attempted_this} ;;
  }

  measure: invoices_attempted_prior_period {
    view_label: "_Period over Period"
    group_label: "Volumes"
    label: "Invoices Attempted - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${fa_attempted_prior} ;;
  }

  measure: invoices_attempted_change_pct {
    view_label: "_Period over Period"
    group_label: "Volumes"
    label: "Invoices Attempted - Change (%)"
    description: "Dashboard only. Percent change vs Prior Period. Group by Invoice Status for outcome-level change."
    type: number
    value_format: "+0.0%;-0.0%;0.0%"
    sql: (${fa_attempted_this} - ${fa_attempted_prior}) / NULLIF(${fa_attempted_prior}, 0)::FLOAT ;;
  }

  # Declined invoices (use with Decline Category / Gateway)
  measure: declined_invoices_this_period {
    view_label: "_Period over Period"
    group_label: "Volumes"
    label: "Declined Invoices - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${fail_inv_this} ;;
  }

  measure: declined_invoices_prior_period {
    view_label: "_Period over Period"
    group_label: "Volumes"
    label: "Declined Invoices - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${fail_inv_prior} ;;
  }

  measure: declined_invoices_change_pct {
    view_label: "_Period over Period"
    group_label: "Volumes"
    label: "Declined Invoices - Change (%)"
    description: "Dashboard only. Percent change vs Prior Period. Group by Decline Category to see which decline types are growing."
    type: number
    value_format: "+0.0%;-0.0%;0.0%"
    sql: (${fail_inv_this} - ${fail_inv_prior}) / NULLIF(${fail_inv_prior}, 0)::FLOAT ;;
  }

  # Authorization rate, all attempts (use with Attempt Bucket for retry change)
  measure: att_this     { hidden: yes type: count filters: [is_payment_event: "yes", pop_period: "This Period"] }
  measure: att_prior    { hidden: yes type: count filters: [is_payment_event: "yes", pop_period: "Prior Period"] }
  measure: att_ok_this  { hidden: yes type: count filters: [event_type: "payment_succeeded", pop_period: "This Period"] }
  measure: att_ok_prior { hidden: yes type: count filters: [event_type: "payment_succeeded", pop_period: "Prior Period"] }

  measure: attempt_auth_rate_this_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Auth Rate (All Attempts) - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${att_ok_this} / NULLIF(${att_this}, 0)::FLOAT ;;
  }

  measure: attempt_auth_rate_prior_period {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Auth Rate (All Attempts) - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${att_ok_prior} / NULLIF(${att_prior}, 0)::FLOAT ;;
  }

  measure: attempt_auth_rate_change {
    view_label: "_Period over Period"
    group_label: "KPIs"
    label: "Auth Rate (All Attempts) - Change (pts)"
    description: "Dashboard only. This Period minus Prior Period, in percentage points. Group by Attempt Bucket for retry effectiveness change."
    type: number
    value_format: "+0.00%;-0.00%;0.00%"
    sql: ${attempt_auth_rate_this_period} - ${attempt_auth_rate_prior_period} ;;
  }

  # Checkout success rate
  measure: ck_vis_this   { hidden: yes type: count_distinct sql: ${anonymous_id} ;; filters: [event_type: "checkout_started", pop_period: "This Period"] }
  measure: ck_vis_prior  { hidden: yes type: count_distinct sql: ${anonymous_id} ;; filters: [event_type: "checkout_started", pop_period: "Prior Period"] }
  measure: ck_done_this  { hidden: yes type: count_distinct sql: ${anonymous_id} ;; filters: [event_type: "checkout_started", checkout_converted: "yes", pop_period: "This Period"] }
  measure: ck_done_prior { hidden: yes type: count_distinct sql: ${anonymous_id} ;; filters: [event_type: "checkout_started", checkout_converted: "yes", pop_period: "Prior Period"] }

  measure: checkout_success_rate_this_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Success Rate - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters. Group by Checkout Card Error Group to split by card error."
    type: number
    value_format_name: percent_2
    sql: ${ck_done_this} / NULLIF(${ck_vis_this}, 0)::FLOAT ;;
  }

  measure: checkout_success_rate_prior_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Success Rate - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${ck_done_prior} / NULLIF(${ck_vis_prior}, 0)::FLOAT ;;
  }

  measure: checkout_success_rate_change {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Success Rate - Change (pts)"
    description: "Dashboard only. This Period minus Prior Period, in percentage points."
    type: number
    value_format: "+0.00%;-0.00%;0.00%"
    sql: ${checkout_success_rate_this_period} - ${checkout_success_rate_prior_period} ;;
  }

  measure: checkout_visitors_this_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Visitors - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${ck_vis_this} ;;
  }

  measure: checkout_visitors_prior_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Visitors - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${ck_vis_prior} ;;
  }

  # Card error recovery rate
  measure: ce_vis_this   { hidden: yes type: count_distinct sql: ${anonymous_id} ;; filters: [event_type: "checkout_card_error", pop_period: "This Period"] }
  measure: ce_vis_prior  { hidden: yes type: count_distinct sql: ${anonymous_id} ;; filters: [event_type: "checkout_card_error", pop_period: "Prior Period"] }
  measure: ce_rec_this   { hidden: yes type: count_distinct sql: ${anonymous_id} ;; filters: [event_type: "checkout_card_error", checkout_converted: "yes", pop_period: "This Period"] }
  measure: ce_rec_prior  { hidden: yes type: count_distinct sql: ${anonymous_id} ;; filters: [event_type: "checkout_card_error", checkout_converted: "yes", pop_period: "Prior Period"] }

  measure: card_error_recovery_rate_this_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Card Error Recovery Rate - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${ce_rec_this} / NULLIF(${ce_vis_this}, 0)::FLOAT ;;
  }

  measure: card_error_recovery_rate_prior_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Card Error Recovery Rate - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${ce_rec_prior} / NULLIF(${ce_vis_prior}, 0)::FLOAT ;;
  }

  measure: card_error_recovery_rate_change {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Card Error Recovery Rate - Change (pts)"
    description: "Dashboard only. This Period minus Prior Period, in percentage points."
    type: number
    value_format: "+0.00%;-0.00%;0.00%"
    sql: ${card_error_recovery_rate_this_period} - ${card_error_recovery_rate_prior_period} ;;
  }

  measure: card_error_visitors_this_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Visitors With a Card Error - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${ce_vis_this} ;;
  }

  measure: card_error_visitors_prior_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Visitors With a Card Error - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${ce_vis_prior} ;;
  }

  # Renewal success, unique customers
  measure: rn_chg_this   { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [is_payment_event: "yes", customer_type: "renewal", pop_period: "This Period"] }
  measure: rn_chg_prior  { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [is_payment_event: "yes", customer_type: "renewal", pop_period: "Prior Period"] }
  measure: rn_paid_this  { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [event_type: "payment_succeeded", customer_type: "renewal", pop_period: "This Period"] }
  measure: rn_paid_prior { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [event_type: "payment_succeeded", customer_type: "renewal", pop_period: "Prior Period"] }
  measure: rn_dec_this   { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [event_type: "payment_failed", customer_type: "renewal", pop_period: "This Period"] }
  measure: rn_dec_prior  { hidden: yes type: count_distinct sql: ${customer_id} ;; filters: [event_type: "payment_failed", customer_type: "renewal", pop_period: "Prior Period"] }

  measure: renewal_customer_success_rate_this_period {
    view_label: "_Period over Period"
    group_label: "Renewal Success"
    label: "Renewal Success Rate (Unique Customers) - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${rn_paid_this} / NULLIF(${rn_chg_this}, 0)::FLOAT ;;
  }

  measure: renewal_customer_success_rate_prior_period {
    view_label: "_Period over Period"
    group_label: "Renewal Success"
    label: "Renewal Success Rate (Unique Customers) - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${rn_paid_prior} / NULLIF(${rn_chg_prior}, 0)::FLOAT ;;
  }

  measure: renewal_customer_success_rate_change {
    view_label: "_Period over Period"
    group_label: "Renewal Success"
    label: "Renewal Success Rate (Unique Customers) - Change (pts)"
    description: "Dashboard only. This Period minus Prior Period, in percentage points."
    type: number
    value_format: "+0.00%;-0.00%;0.00%"
    sql: ${renewal_customer_success_rate_this_period} - ${renewal_customer_success_rate_prior_period} ;;
  }

  measure: renewal_customer_first_try_rate_this_period {
    view_label: "_Period over Period"
    group_label: "Renewal Success"
    label: "Renewal First-Try Rate (Unique Customers) - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: (${rn_chg_this} - ${rn_dec_this}) / NULLIF(${rn_chg_this}, 0)::FLOAT ;;
  }

  measure: renewal_customer_first_try_rate_prior_period {
    view_label: "_Period over Period"
    group_label: "Renewal Success"
    label: "Renewal First-Try Rate (Unique Customers) - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: (${rn_chg_prior} - ${rn_dec_prior}) / NULLIF(${rn_chg_prior}, 0)::FLOAT ;;
  }

  measure: renewal_customer_first_try_rate_change {
    view_label: "_Period over Period"
    group_label: "Renewal Success"
    label: "Renewal First-Try Rate (Unique Customers) - Change (pts)"
    description: "Dashboard only. This Period minus Prior Period, in percentage points."
    type: number
    value_format: "+0.00%;-0.00%;0.00%"
    sql: ${renewal_customer_first_try_rate_this_period} - ${renewal_customer_first_try_rate_prior_period} ;;
  }

  # Checkout card errors (use with Checkout Error Category / Reason)
  measure: card_errors_this_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Card Errors - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: count
    filters: [event_type: "checkout_card_error", pop_period: "This Period"]
  }

  measure: card_errors_prior_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Card Errors - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: count
    filters: [event_type: "checkout_card_error", pop_period: "Prior Period"]
  }

  measure: card_errors_change_pct {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Card Errors - Change (%)"
    description: "Dashboard only. Percent change vs Prior Period. Group by Checkout Error Category to see which errors are growing."
    type: number
    value_format: "+0.0%;-0.0%;0.0%"
    sql: (${card_errors_this_period} - ${card_errors_prior_period}) / NULLIF(${card_errors_prior_period}, 0)::FLOAT ;;
  }

  # Checkout payment success (orders vs card errors, events)
  measure: orders_this  { hidden: yes type: count filters: [event_type: "order_completed", pop_period: "This Period"] }
  measure: orders_prior { hidden: yes type: count filters: [event_type: "order_completed", pop_period: "Prior Period"] }

  measure: orders_completed_this_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Completed Orders - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${orders_this} ;;
  }

  measure: orders_completed_prior_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Completed Orders - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    sql: ${orders_prior} ;;
  }

  measure: checkout_payment_success_rate_this_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Payment Success Rate - This Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters. Completed Orders / (Completed Orders + Checkout Card Errors), this period."
    type: number
    value_format_name: percent_2
    sql: ${orders_this} / NULLIF(${orders_this} + ${card_errors_this_period}, 0)::FLOAT ;;
  }

  measure: checkout_payment_success_rate_prior_period {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Payment Success Rate - Prior Period"
    description: "Dashboard only; requires the Current Period and Prior Period filters."
    type: number
    value_format_name: percent_2
    sql: ${orders_prior} / NULLIF(${orders_prior} + ${card_errors_prior_period}, 0)::FLOAT ;;
  }

  measure: checkout_payment_success_rate_change {
    view_label: "_Period over Period"
    group_label: "Checkout"
    label: "Checkout Payment Success Rate - Change (pts)"
    description: "Dashboard only. This Period minus Prior Period, in percentage points."
    type: number
    value_format: "+0.00%;-0.00%;0.00%"
    sql: ${checkout_payment_success_rate_this_period} - ${checkout_payment_success_rate_prior_period} ;;
  }

  # ===========================================================================
  # FIELD SETS
  # ===========================================================================

  # Record-level drill used by the count measures
  set: detail {
    fields: [
      event_et_time, event_type, customer_id, subscription_id, invoice_id,
      gateway, billing_period, customer_type, attempt_number,
      error_text, error_category, invoice_status
    ]
  }

  # ip_address is deliberately left out of ca_fields (personal data).

  # Liquid-dependent fields; keep out of the conversational explore
  set: pop_fields {
    fields: [
      pop_date_range, pop_prior_date_range, pop_period, pop_day_index,
      first_attempt_auth_rate_this_period, first_attempt_auth_rate_prior_period, first_attempt_auth_rate_change,
      dunning_rate_this_period, dunning_rate_prior_period, dunning_rate_change,
      recovery_rate_this_period, recovery_rate_prior_period, recovery_rate_change,
      involuntary_churn_this_period, involuntary_churn_prior_period, involuntary_churn_change_pct,
      revenue_not_recovered_this_period, revenue_not_recovered_prior_period, revenue_not_recovered_change_pct,
      revenue_at_risk_this_period, revenue_at_risk_prior_period, revenue_at_risk_change_pct,
      invoices_attempted_this_period, invoices_attempted_prior_period, invoices_attempted_change_pct,
      declined_invoices_this_period, declined_invoices_prior_period, declined_invoices_change_pct,
      attempt_auth_rate_this_period, attempt_auth_rate_prior_period, attempt_auth_rate_change,
      checkout_success_rate_this_period, checkout_success_rate_prior_period, checkout_success_rate_change,
      checkout_visitors_this_period, checkout_visitors_prior_period,
      card_error_recovery_rate_this_period, card_error_recovery_rate_prior_period, card_error_recovery_rate_change,
      card_error_visitors_this_period, card_error_visitors_prior_period,
      card_errors_this_period, card_errors_prior_period, card_errors_change_pct,
      orders_completed_this_period, orders_completed_prior_period,
      checkout_payment_success_rate_this_period, checkout_payment_success_rate_prior_period, checkout_payment_success_rate_change,
      renewal_customer_success_rate_this_period, renewal_customer_success_rate_prior_period, renewal_customer_success_rate_change,
      renewal_customer_first_try_rate_this_period, renewal_customer_first_try_rate_prior_period, renewal_customer_first_try_rate_change
    ]
  }

  # Everything the CA agent should see (explore: fields: [payment_gateway.ca_fields*])
  set: ca_fields {
    fields: [
      # when
      report_date, report_week, report_month, report_quarter, report_year,
      report_day_of_week, report_day_of_month,
      event_et_time, event_et_hour_of_day, event_et_day_of_week,
      # who / where
      event_type, gateway, billing_period, customer_type, churn_cohort, cancel_reason,
      card_type, card_funding_type, has_stripe_backup,
      # signup model
      signup_cohort, renewal_stage, days_since_activation,
      subscription_created_date, subscription_created_week, subscription_created_month,
      # why
      error_category, error_text, error_code, suggested_action,
      # retries and outcome
      attempt_number, attempt_bucket, invoice_attempt_count, invoice_status,
      invoice_had_failure, invoice_paid, recovered_via_gateway,
      recovered_on_alternate_gateway, hours_to_recover,
      # drill ids
      customer_id, subscription_id, invoice_id,
      # measures
      charge_attempts, successful_charges, failed_charges, attempt_auth_rate,
      invoices_attempted, first_attempt_successes, first_attempt_auth_rate,
      invoices_paid, eventual_auth_rate,
      invoices_with_failure, invoices_recovered, invoices_still_in_dunning,
      invoices_not_recovered, recovery_rate, invoices_recovered_on_alternate_gateway,
      avg_attempts_to_recover, median_hours_to_recover,
      revenue_at_risk, revenue_recovered, revenue_not_recovered,
      dunning_users, renewed_users, dunning_rate,
      involuntary_churn_users, involuntary_churn_trial, involuntary_churn_renew,
      # checkout and renewal success
      checkout_converted, checkout_had_card_error, checkout_card_error_group, anonymous_id,
      checkout_visitors, checkout_completers, checkout_success_rate,
      card_errors, card_error_visitors, card_error_recovered_visitors, card_error_recovery_rate,
      checkout_error_category, checkout_error_reason, checkout_error_code, checkout_error_owner,
      checkout_error_action, checkout_error_message, card_error_ip_addresses, card_errors_per_visitor,
      orders_completed, checkout_payment_success_rate,
      renewal_charge_customers, renewal_paid_customers, renewal_declined_customers,
      renewal_customer_success_rate, renewal_customer_first_try_rate
    ]
  }
}
