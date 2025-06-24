view: chargebee_subscriptions {

  derived_table: {
    sql: SELECT * FROM `up-faith-and-family-216419.chargebee.subscriptions` WHERE date(uploaded_at) = CURRENT_DATE() ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: customer_billing_address_state {
    type: string
    sql: ${TABLE}.customer_billing_address_state ;;
  }

  dimension: card_last4 {
    type: number
    sql: ${TABLE}.card_last4 ;;
  }

  dimension: card_gateway_account_id {
    type: string
    sql: ${TABLE}.card_gateway_account_id ;;
  }

  dimension: subscription_currency_code {
    type: string
    sql: ${TABLE}.subscription_currency_code ;;
  }

  dimension: subscription_subscription_items_0_item_price_id {
    type: string
    sql: ${TABLE}.subscription_subscription_items_0_item_price_id ;;
  }

  dimension: card_first_name {
    type: string
    sql: ${TABLE}.card_first_name ;;
  }

  dimension: card_billing_zip {
    type: string
    sql: ${TABLE}.card_billing_zip ;;
  }

  dimension: card_customer_id {
    type: string
    sql: ${TABLE}.card_customer_id ;;
  }

  dimension: subscription_cancel_reason_code {
    type: string
    sql: ${TABLE}.subscription_cancel_reason_code ;;
  }

  dimension: subscription_subscription_items_1_object {
    type: string
    sql: ${TABLE}.subscription_subscription_items_1_object ;;
  }

  dimension: subscription_subscription_items_1_item_type {
    type: string
    sql: ${TABLE}.subscription_subscription_items_1_item_type ;;
  }

  dimension: subscription_created_at {
    type: number
    sql: ${TABLE}.subscription_created_at ;;
  }

  dimension: customer_email {
    type: string
    sql: ${TABLE}.customer_email ;;
  }

  dimension: subscription_subscription_items_1_quantity {
    type: number
    sql: ${TABLE}.subscription_subscription_items_1_quantity ;;
  }

  dimension: subscription_base_currency_code {
    type: string
    sql: ${TABLE}.subscription_base_currency_code ;;
  }

  dimension: subscription_total_dues {
    type: number
    sql: ${TABLE}.subscription_total_dues ;;
  }

  dimension: subscription_due_since {
    type: number
    sql: ${TABLE}.subscription_due_since ;;
  }

  dimension: subscription_cf_legacy_customers {
    type: yesno
    sql: ${TABLE}.subscription_cf_legacy_customers ;;
  }

  dimension: subscription_resource_version {
    type: number
    sql: ${TABLE}.subscription_resource_version ;;
  }

  dimension: subscription_cancel_reason {
    type: string
    sql: ${TABLE}.subscription_cancel_reason ;;
  }

  dimension: subscription_subscription_items_0_object {
    type: string
    sql: ${TABLE}.subscription_subscription_items_0_object ;;
  }

  dimension: card_last_name {
    type: string
    sql: ${TABLE}.card_last_name ;;
  }

  dimension: subscription_channel {
    type: string
    sql: ${TABLE}.subscription_channel ;;
  }

  dimension: card_issuing_country {
    type: string
    sql: ${TABLE}.card_issuing_country ;;
  }

  dimension: card_expiry_year {
    type: number
    sql: ${TABLE}.card_expiry_year ;;
  }

  dimension: customer_first_name {
    type: string
    sql: ${TABLE}.customer_first_name ;;
  }

  dimension: subscription_exchange_rate {
    type: number
    sql: ${TABLE}.subscription_exchange_rate ;;
  }

  dimension: subscription_has_scheduled_advance_invoices {
    type: yesno
    sql: ${TABLE}.subscription_has_scheduled_advance_invoices ;;
  }

  dimension: customer_payment_method_object {
    type: string
    sql: ${TABLE}.customer_payment_method_object ;;
  }

  dimension: customer_taxability {
    type: string
    sql: ${TABLE}.customer_taxability ;;
  }

  dimension: subscription_subscription_items_0_unit_price {
    type: number
    sql: ${TABLE}.subscription_subscription_items_0_unit_price ;;
  }

  dimension: customer_channel {
    type: string
    sql: ${TABLE}.customer_channel ;;
  }

  dimension: customer_billing_address_validation_status {
    type: string
    sql: ${TABLE}.customer_billing_address_validation_status ;;
  }

  dimension: customer_promotional_credits {
    type: number
    sql: ${TABLE}.customer_promotional_credits ;;
  }

  dimension: subscription_started_at {
    type: number
    sql: ${TABLE}.subscription_started_at ;;
  }

  dimension: customer_card_status {
    type: string
    sql: ${TABLE}.customer_card_status ;;
  }

  dimension: subscription_subscription_items_1_amount {
    type: number
    sql: ${TABLE}.subscription_subscription_items_1_amount ;;
  }

  dimension: subscription_coupons_0_apply_till {
    type: number
    sql: ${TABLE}.subscription_coupons_0_apply_till ;;
  }

  dimension: card_billing_state {
    type: string
    sql: ${TABLE}.card_billing_state ;;
  }

  dimension: customer_object {
    type: string
    sql: ${TABLE}.customer_object ;;
  }

  dimension: subscription_activated_at {
    type: number
    sql: ${TABLE}.subscription_activated_at ;;
  }

  dimension: subscription_coupons_0_applied_count {
    type: number
    sql: ${TABLE}.subscription_coupons_0_applied_count ;;
  }

  dimension: customer_refundable_credits {
    type: number
    sql: ${TABLE}.customer_refundable_credits ;;
  }

  dimension: subscription_current_term_start {
    type: number
    sql: ${TABLE}.subscription_current_term_start ;;
  }

  dimension: card_funding_type {
    type: string
    sql: ${TABLE}.card_funding_type ;;
  }

  dimension: subscription_coupon {
    type: string
    sql: ${TABLE}.subscription_coupon ;;
  }

  dimension: subscription_subscription_items_0_amount {
    type: number
    sql: ${TABLE}.subscription_subscription_items_0_amount ;;
  }

  dimension: card_object {
    type: string
    sql: ${TABLE}.card_object ;;
  }

  dimension: customer_mrr {
    type: number
    sql: ${TABLE}.customer_mrr ;;
  }

  dimension: subscription_cancel_schedule_created_at {
    type: number
    sql: ${TABLE}.subscription_cancel_schedule_created_at ;;
  }

  dimension: customer_auto_collection {
    type: string
    sql: ${TABLE}.customer_auto_collection ;;
  }

  dimension: subscription_updated_at {
    type: number
    sql: ${TABLE}.subscription_updated_at ;;
  }

  dimension: card_updated_at {
    type: number
    sql: ${TABLE}.card_updated_at ;;
  }

  dimension: customer_excess_payments {
    type: number
    sql: ${TABLE}.customer_excess_payments ;;
  }

  dimension: subscription_subscription_items_0_quantity {
    type: number
    sql: ${TABLE}.subscription_subscription_items_0_quantity ;;
  }

  dimension: customer_billing_address_state_code {
    type: string
    sql: ${TABLE}.customer_billing_address_state_code ;;
  }

  dimension: card_expiry_month {
    type: number
    sql: ${TABLE}.card_expiry_month ;;
  }

  dimension: card_iin {
    type: string
    sql: ${TABLE}.card_iin ;;
  }

  dimension: subscription_trial_start {
    type: number
    sql: ${TABLE}.subscription_trial_start ;;
  }

  dimension: subscription_mrr {
    type: number
    sql: ${TABLE}.subscription_mrr ;;
  }

  dimension: customer_payment_method_gateway_account_id {
    type: string
    sql: ${TABLE}.customer_payment_method_gateway_account_id ;;
  }

  dimension: subscription_has_scheduled_changes {
    type: yesno
    sql: ${TABLE}.subscription_has_scheduled_changes ;;
  }

  dimension: customer_unbilled_charges {
    type: number
    sql: ${TABLE}.customer_unbilled_charges ;;
  }

  dimension: subscription_customer_id {
    type: string
    sql: ${TABLE}.subscription_customer_id ;;
  }

  dimension: card_masked_number {
    type: string
    sql: ${TABLE}.card_masked_number ;;
  }

  dimension: card_status {
    type: string
    sql: ${TABLE}.card_status ;;
  }

  dimension: customer_created_at {
    type: number
    sql: ${TABLE}.customer_created_at ;;
  }

  dimension: customer_deleted {
    type: yesno
    sql: ${TABLE}.customer_deleted ;;
  }

  dimension: subscription_next_billing_at {
    type: number
    sql: ${TABLE}.subscription_next_billing_at ;;
  }

  dimension: card_resource_version {
    type: number
    sql: ${TABLE}.card_resource_version ;;
  }

  dimension: subscription_coupons_0_object {
    type: string
    sql: ${TABLE}.subscription_coupons_0_object ;;
  }

  dimension: subscription_created_from_ip {
    type: string
    sql: ${TABLE}.subscription_created_from_ip ;;
  }

  dimension: customer_created_from_ip {
    type: string
    sql: ${TABLE}.customer_created_from_ip ;;
  }

  dimension: subscription_subscription_items_1_unit_price {
    type: number
    sql: ${TABLE}.subscription_subscription_items_1_unit_price ;;
  }

  dimension: subscription_subscription_items_0_free_quantity {
    type: number
    sql: ${TABLE}.subscription_subscription_items_0_free_quantity ;;
  }

  dimension: subscription_coupons_0_coupon_code {
    type: string
    sql: ${TABLE}.subscription_coupons_0_coupon_code ;;
  }

  dimension: customer_resource_version {
    type: number
    sql: ${TABLE}.customer_resource_version ;;
  }

  dimension: subscription_billing_period {
    type: number
    sql: ${TABLE}.subscription_billing_period ;;
  }

  dimension: customer_fraud_flag {
    type: string
    sql: ${TABLE}.customer_fraud_flag ;;
  }

  dimension: customer_cs_marketing_opt_in {
    type: yesno
    sql: ${TABLE}.customer_cs_marketing_opt_in ;;
  }

  dimension: subscription_deleted {
    type: yesno
    sql: ${TABLE}.subscription_deleted ;;
  }

  dimension: subscription_remaining_billing_cycles {
    type: number
    sql: ${TABLE}.subscription_remaining_billing_cycles ;;
  }

  dimension: subscription_coupons_0_coupon_id {
    type: string
    sql: ${TABLE}.subscription_coupons_0_coupon_id ;;
  }

  dimension: customer_net_term_days {
    type: number
    sql: ${TABLE}.customer_net_term_days ;;
  }

  dimension: card_gateway {
    type: string
    sql: ${TABLE}.card_gateway ;;
  }

  dimension: customer_billing_address_country {
    type: string
    sql: ${TABLE}.customer_billing_address_country ;;
  }

  dimension: subscription_trial_end {
    type: number
    sql: ${TABLE}.subscription_trial_end ;;
  }

  dimension: subscription_billing_period_unit {
    type: string
    sql: ${TABLE}.subscription_billing_period_unit ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: card_powered_by {
    type: string
    sql: ${TABLE}.card_powered_by ;;
  }

  dimension: subscription_cancelled_at {
    type: number
    sql: ${TABLE}.subscription_cancelled_at ;;
  }

  dimension: customer_customer_type {
    type: string
    sql: ${TABLE}.customer_customer_type ;;
  }

  dimension: subscription_subscription_items_0_trial_end {
    type: number
    sql: ${TABLE}.subscription_subscription_items_0_trial_end ;;
  }

  dimension: card_created_at {
    type: number
    sql: ${TABLE}.card_created_at ;;
  }

  dimension: card_billing_state_code {
    type: string
    sql: ${TABLE}.card_billing_state_code ;;
  }

  dimension: card_payment_source_id {
    type: string
    sql: ${TABLE}.card_payment_source_id ;;
  }

  dimension: customer_payment_method_gateway {
    type: string
    sql: ${TABLE}.customer_payment_method_gateway ;;
  }

  dimension: subscription_subscription_items_0_billing_cycles {
    type: number
    sql: ${TABLE}.subscription_subscription_items_0_billing_cycles ;;
  }

  dimension: subscription_current_term_end {
    type: number
    sql: ${TABLE}.subscription_current_term_end ;;
  }

  dimension: customer_billing_address_object {
    type: string
    sql: ${TABLE}.customer_billing_address_object ;;
  }

  dimension: card_billing_country {
    type: string
    sql: ${TABLE}.card_billing_country ;;
  }

  dimension: customer_payment_method_reference_id {
    type: string
    sql: ${TABLE}.customer_payment_method_reference_id ;;
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: customer_pii_cleared {
    type: string
    sql: ${TABLE}.customer_pii_cleared ;;
  }

  dimension: customer_payment_method_status {
    type: string
    sql: ${TABLE}.customer_payment_method_status ;;
  }

  dimension: customer_updated_at {
    type: number
    sql: ${TABLE}.customer_updated_at ;;
  }

  dimension: subscription_subscription_items_0_item_type {
    type: string
    sql: ${TABLE}.subscription_subscription_items_0_item_type ;;
  }

  dimension: customer_preferred_currency_code {
    type: string
    sql: ${TABLE}.customer_preferred_currency_code ;;
  }

  dimension: customer_payment_method_type {
    type: string
    sql: ${TABLE}.customer_payment_method_type ;;
  }

  dimension: subscription_object {
    type: string
    sql: ${TABLE}.subscription_object ;;
  }

  dimension: subscription_subscription_items_1_item_price_id {
    type: string
    sql: ${TABLE}.subscription_subscription_items_1_item_price_id ;;
  }

  dimension: customer_billing_address_zip {
    type: string
    sql: ${TABLE}.customer_billing_address_zip ;;
  }

  dimension: customer_last_name {
    type: string
    sql: ${TABLE}.customer_last_name ;;
  }

  dimension: customer_allow_direct_debit {
    type: yesno
    sql: ${TABLE}.customer_allow_direct_debit ;;
  }

  dimension: subscription_due_invoices_count {
    type: number
    sql: ${TABLE}.subscription_due_invoices_count ;;
  }

  dimension: customer_primary_payment_source_id {
    type: string
    sql: ${TABLE}.customer_primary_payment_source_id ;;
  }

  dimension: card_ip_address {
    type: string
    sql: ${TABLE}.card_ip_address ;;
  }

  dimension: card_card_type {
    type: string
    sql: ${TABLE}.card_card_type ;;
  }

  dimension: uploaded_at {
    type: date
    datatype: date
    sql: ${TABLE}.uploaded_at ;;
  }

  set: detail {
    fields: [
      subscription_id,
      customer_billing_address_state,
      card_last4,
      card_gateway_account_id,
      subscription_currency_code,
      subscription_subscription_items_0_item_price_id,
      card_first_name,
      card_billing_zip,
      card_customer_id,
      subscription_cancel_reason_code,
      subscription_subscription_items_1_object,
      subscription_subscription_items_1_item_type,
      subscription_created_at,
      customer_email,
      subscription_subscription_items_1_quantity,
      subscription_base_currency_code,
      subscription_total_dues,
      subscription_due_since,
      subscription_cf_legacy_customers,
      subscription_resource_version,
      subscription_cancel_reason,
      subscription_subscription_items_0_object,
      card_last_name,
      subscription_channel,
      card_issuing_country,
      card_expiry_year,
      customer_first_name,
      subscription_exchange_rate,
      subscription_has_scheduled_advance_invoices,
      customer_payment_method_object,
      customer_taxability,
      subscription_subscription_items_0_unit_price,
      customer_channel,
      customer_billing_address_validation_status,
      customer_promotional_credits,
      subscription_started_at,
      customer_card_status,
      subscription_subscription_items_1_amount,
      subscription_coupons_0_apply_till,
      card_billing_state,
      customer_object,
      subscription_activated_at,
      subscription_coupons_0_applied_count,
      customer_refundable_credits,
      subscription_current_term_start,
      card_funding_type,
      subscription_coupon,
      subscription_subscription_items_0_amount,
      card_object,
      customer_mrr,
      subscription_cancel_schedule_created_at,
      customer_auto_collection,
      subscription_updated_at,
      card_updated_at,
      customer_excess_payments,
      subscription_subscription_items_0_quantity,
      customer_billing_address_state_code,
      card_expiry_month,
      card_iin,
      subscription_trial_start,
      subscription_mrr,
      customer_payment_method_gateway_account_id,
      subscription_has_scheduled_changes,
      customer_unbilled_charges,
      subscription_customer_id,
      card_masked_number,
      card_status,
      customer_created_at,
      customer_deleted,
      subscription_next_billing_at,
      card_resource_version,
      subscription_coupons_0_object,
      subscription_created_from_ip,
      customer_created_from_ip,
      subscription_subscription_items_1_unit_price,
      subscription_subscription_items_0_free_quantity,
      subscription_coupons_0_coupon_code,
      customer_resource_version,
      subscription_billing_period,
      customer_fraud_flag,
      customer_cs_marketing_opt_in,
      subscription_deleted,
      subscription_remaining_billing_cycles,
      subscription_coupons_0_coupon_id,
      customer_net_term_days,
      card_gateway,
      customer_billing_address_country,
      subscription_trial_end,
      subscription_billing_period_unit,
      subscription_status,
      card_powered_by,
      subscription_cancelled_at,
      customer_customer_type,
      subscription_subscription_items_0_trial_end,
      card_created_at,
      card_billing_state_code,
      card_payment_source_id,
      customer_payment_method_gateway,
      subscription_subscription_items_0_billing_cycles,
      subscription_current_term_end,
      customer_billing_address_object,
      card_billing_country,
      customer_payment_method_reference_id,
      customer_id,
      customer_pii_cleared,
      customer_payment_method_status,
      customer_updated_at,
      subscription_subscription_items_0_item_type,
      customer_preferred_currency_code,
      customer_payment_method_type,
      subscription_object,
      subscription_subscription_items_1_item_price_id,
      customer_billing_address_zip,
      customer_last_name,
      customer_allow_direct_debit,
      subscription_due_invoices_count,
      customer_primary_payment_source_id,
      card_ip_address,
      card_card_type,
      uploaded_at
    ]
  }

}
