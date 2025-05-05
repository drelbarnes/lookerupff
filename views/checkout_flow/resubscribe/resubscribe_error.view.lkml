view: resubscribe_error {
  derived_table: {
    sql:
      select
        context_ip
        ,api_error_code
        ,CASE
          WHEN message like 'Subscription cannot%Insufficient funds%' THEN 'Error message: (3001) Insufficient funds.'
          WHEN message like 'Subscription cannot%insufficient funds%' THEN 'Error message: (card_declined) Your card has insufficient funds.'
          WHEN message like 'Subscription cannot%Your card was declined%' THEN 'Error message: (card_declined) Your card was declined.'
          WHEN message like '%Your card does not support this type of purchase%' THEN 'Error message: (card_declined) Your card does not support this type of purchase.'
          WHEN message like '%Your card number is incorrect%' THEN 'Error message: (incorrect_number) Your card number is incorrect.'
          WHEN message like '%Error message: (card_declined) Invalid account%'THEN 'Error message: (card_declined) Invalid account.'
          WHEN message like '%The instrument presented  was either declined by the processor or bank%'THEN 'Error message: (INSTRUMENT_DECLINED) The instrument presented  was either declined by the processor or bank, or it cant be used for this payment.'
          WHEN message like '%Your card has expired%'THEN 'Error message: (expired_card) Your card has expired.'
          WHEN message like '%(TRANSACTION_REFUSED) The request was refuse%'THEN 'Error message: (TRANSACTION_REFUSED) The request was refused.'
          WHEN message like '%(AGREEMENT_ALREADY_CANCELLED) The requested agreement is already canceled%' THEN 'Error message: (AGREEMENT_ALREADY_CANCELLED) The requested agreement is already canceled.'
          WHEN message like '%Error message: (processing_error) An error occurred while processing your card. Try again in a little bit%'THEN 'Error message: (processing_error) An error occurred while processing your card. Try again in a little bit.'
          WHEN message like '%Payer cannot pay for this transaction%' THEN 'Error message: (PAYER_CANNOT_PAY) Payer cannot pay for this transaction. Please contact the payer to find other ways to pay for this transaction.'
          WHEN message like '%(INVALID_STRING_LENGTH) The value of a field%'THEN 'Error message: (INVALID_STRING_LENGTH) The value of a field is either too short or too long.'
          Else message
          END AS message
        ,timestamp
      from javascript_upentertainment_checkout.resubscribe_error
      where api_error_code != 'invalid_state_for_request';;
      }

  dimension: date {
      type: date
      sql: ${TABLE}.timestamp ;;
  }


  dimension: ip_address {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: message {
    type: string
    sql: ${TABLE}.message ;;
  }

  dimension: api_error_code {
    type: string
    sql:  ${TABLE}.api_error_code ;;
  }

  measure: error_code_count {
    type: count_distinct
    sql:${TABLE}.context_ip;;

    label: "API Error Code Count"
  }

  measure: error_message_count {
    type: count_distinct
    sql:${TABLE}.context_ip;;

    label: "API Error Message Count"
  }





}
