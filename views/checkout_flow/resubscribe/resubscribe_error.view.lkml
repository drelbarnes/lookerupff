view: resubscribe_error {
  derived_table: {
    sql:
    with result as(
      select
        context_ip
        ,CASE
    WHEN POSITION('rid' IN context_page_url) > 0 THEN
      CASE
        WHEN POSITION('&' IN SUBSTRING(context_page_url FROM POSITION('rid' IN context_page_url))) > 0 THEN
          SUBSTRING(
            context_page_url,
            POSITION('rid' IN context_page_url),
            POSITION('&' IN SUBSTRING(context_page_url FROM POSITION('rid' IN context_page_url))) - 1
          )
        ELSE
          SUBSTRING(context_page_url FROM POSITION('rid' IN context_page_url))
      END
    ELSE NULL
        END AS rid
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
      where api_error_code != 'invalid_state_for_request' and api_error_code != 'configuration_incompatible')

      select
      context_ip
      ,CASE
        WHEN (rid is NULL or rid = '') THEN context_ip
        ELSE rid
        END AS rid
      ,api_error_code
      ,message
      ,timestamp
      from result
      ;;
      }

  dimension: date {
      type: date
      sql: ${TABLE}.timestamp ;;
  }


  dimension: ip_address {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: rid {
    type: string
    sql: ${TABLE}.rid ;;
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
    sql:${TABLE}.rid;;

    label: "API Error Code Count"
  }

  measure: error_message_count {
    type: count_distinct
    sql:${TABLE}.rid;;

    label: "API Error Message Count"
  }





}
