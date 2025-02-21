view: ovation_revenue {
dimension: subscription_id {
  type: string
  sql: ${TABLE}.subscription_id ;;
  description: "Unique identifier of the customer-held subscription."
}

dimension: date {
  type: date
  sql: ${TABLE}.date ;;
  description: "Date of the transaction."
}

dimension: term_start_date {
  type: date
  sql: ${TABLE}.term_start_date ;;
  description: "Start date of the revenue period, inclusive, starting at midnight of this date."
}

dimension: term_end_date {
  type: date
  sql: ${TABLE}.term_end_date ;;
  description: "End date of the revenue period, exclusive, up to midnight of this date."
}

dimension: product_id {
  type: string
  sql: ${TABLE}.product_id ;;
  description: "The ID of the product that was sold."
}

dimension: revenue_amount {
  type: number
  sql: ${TABLE}.revenue_amount ;;
  description: "The amount of currency in the transaction."
}

dimension: currency {
  type: string
  sql: ${TABLE}.currency ;;
  description: "String holding an ISO 4217:2015 currency code."}
}
