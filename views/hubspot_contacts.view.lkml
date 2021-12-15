# The name of this view in Looker is "Hubspot Contacts"
view: hubspot_contacts {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: `up-faith-and-family-216419.hubspot.contacts`
    ;;
  drill_fields: [email]
  # This primary key is the unique key for this table in the underlying database.
  # You need to define a primary key in a view in order to join to other views.

  dimension: email {
    primary_key: yes
    type: string
    sql: ${TABLE}.email ;;
  }

  # dimension: id {
  #   primary_key: yes
  #   type: string
  #   sql: ${TABLE}.id ;;
  # }

  # Dates and timestamps can be represented in Looker using a dimension group of type: time.
  # Looker converts dates and timestamps to the specified timeframes within the dimension group.

  # dimension_group: _partitiondate {
  #   type: time
  #   timeframes: [
  #     raw,
  #     date,
  #     week,
  #     month,
  #     quarter,
  #     year
  #   ]
  #   convert_tz: no
  #   datatype: date
  #   sql: ${TABLE}._PARTITIONDATE ;;
  # }

  # dimension_group: _partitiontime {
  #   type: time
  #   timeframes: [
  #     raw,
  #     date,
  #     week,
  #     month,
  #     quarter,
  #     year
  #   ]
  #   convert_tz: no
  #   datatype: date
  #   sql: ${TABLE}._PARTITIONTIME ;;
  # }

  # dimension_group: added {
  #   type: time
  #   timeframes: [
  #     raw,
  #     time,
  #     date,
  #     week,
  #     month,
  #     quarter,
  #     year
  #   ]
  #   sql: ${TABLE}.added_at ;;
  # }

  # Here's what a typical dimension looks like in LookML.
  # A dimension is a groupable field that can be used to filter query results.
  # This dimension will be called "Canonical Vid" in Explore.

  # dimension: canonical_vid {
  #   type: number
  #   value_format_name: id
  #   sql: ${TABLE}.canonical_vid ;;
  # }

  # dimension: form_submissions {
  #   type: string
  #   sql: ${TABLE}.form_submissions ;;
  # }

  # dimension: is_contact {
  #   type: yesno
  #   sql: ${TABLE}.is_contact ;;
  # }

  # dimension: lead_guid {
  #   type: string
  #   sql: ${TABLE}.lead_guid ;;
  # }

  # dimension: list_memberships {
  #   type: string
  #   sql: ${TABLE}.list_memberships ;;
  # }

  # dimension_group: loaded {
  #   type: time
  #   timeframes: [
  #     raw,
  #     time,
  #     date,
  #     week,
  #     month,
  #     quarter,
  #     year
  #   ]
  #   sql: ${TABLE}.loaded_at ;;
  # }

  # dimension: portal_id {
  #   type: number
  #   sql: ${TABLE}.portal_id ;;
  # }

  dimension: properties_churn_score_value {
    type: string
    sql: ${TABLE}.properties_churn_score_value ;;
  }

  dimension: properties_company_value {
    type: string
    sql: ${TABLE}.properties_company_value ;;
  }

  dimension: properties_firstname_value {
    type: string
    sql: ${TABLE}.properties_firstname_value ;;
  }

  dimension: properties_frequency_value {
    type: string
    sql: ${TABLE}.properties_frequency_value ;;
  }

  dimension: properties_lastmodifieddate_value {
    type: string
    sql: ${TABLE}.properties_lastmodifieddate_value ;;
  }

  dimension_group: last_modified {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: CAST(${TABLE}.properties_lastmodifieddate_value as INTEGER);;
  }

  dimension: properties_lastname_value {
    type: string
    sql: ${TABLE}.properties_lastname_value ;;
  }

  dimension: properties_moptin_value {
    type: string
    sql: ${TABLE}.properties_moptin_value ;;
  }

  dimension: properties_subscriber_marketing_opt_in_value {
    type: string
    sql: ${TABLE}.properties_subscriber_marketing_opt_in_value ;;
  }

  dimension: properties_subscription_status_value {
    type: string
    sql: ${TABLE}.properties_subscription_status_value ;;
  }

  dimension: properties_topic_value {
    type: string
    sql: ${TABLE}.properties_topic_value ;;
  }

  dimension: properties_vod_brand_value {
    type: string
    sql: ${TABLE}.properties_vod_brand_value ;;
  }

  # dimension_group: received {
  #   type: time
  #   timeframes: [
  #     raw,
  #     time,
  #     date,
  #     week,
  #     month,
  #     quarter,
  #     year
  #   ]
  #   sql: ${TABLE}.received_at ;;
  # }

  # dimension_group: uuid_ts {
  #   type: time
  #   timeframes: [
  #     raw,
  #     time,
  #     date,
  #     week,
  #     month,
  #     quarter,
  #     year
  #   ]
  #   sql: ${TABLE}.uuid_ts ;;
  # }

  measure: count {
    type: count
    drill_fields: [email]
  }
}
