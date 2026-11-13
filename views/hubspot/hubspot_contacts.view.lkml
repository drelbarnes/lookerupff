###############################################################################
# HubSpot Contacts  (view 3 of 3)
# -----------------------------------------------------------------------------
# One row per email address in HubSpot (valid, un-merged contacts only).
# Joined to Email Activity by email, and also its own explore for audience size.
#
# Built for speed: distributed and sorted on email, the same distribution key
# as Email Activity, so the join runs on each node without moving data.
###############################################################################

view: hubspot_contacts {
  label: "Contacts"

  derived_table: {
    datagroup_trigger: hubspot_segment_sync
    distribution: "email"
    sortkeys: ["email"]
    sql:
      SELECT x.contact_id, x.email, x.first_name, x.last_name, x.company, x.added_at, x.portal_id
      FROM (
          SELECT
              c.id::VARCHAR                       AS contact_id,
              LOWER(TRIM(c.email))                AS email,
              c.properties_firstname_value        AS first_name,
              c.properties_lastname_value         AS last_name,
              c.properties_company_value          AS company,
              CONVERT_TIMEZONE('UTC', 'America/New_York',
                  CASE WHEN c.properties_createdate_value::VARCHAR ~ '^[0-9]{12,13}$'
                    THEN TIMESTAMP 'epoch' + (c.properties_createdate_value::VARCHAR::BIGINT / 1000) * INTERVAL '1 second'
                    ELSE CAST(c.properties_createdate_value::VARCHAR AS TIMESTAMP) END) AS added_at,  -- HubSpot create date (not Segment's sync-time added_at)
              c.portal_id::VARCHAR                AS portal_id,
              ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(c.email)) ORDER BY c.id) AS rn
          FROM hubspot.contacts c
          WHERE c.email IS NOT NULL
            AND COALESCE(c.is_contact, TRUE)
            AND (c.canonical_vid IS NULL OR c.canonical_vid::VARCHAR = c.id::VARCHAR)
      ) x
      WHERE x.rn = 1
    ;;
  }

  dimension: email {
    primary_key: yes
    type: string
    label: "Contact Email"
    description: "The contact's email address. Personal data: do not export or share outside the marketing team."
    tags: ["pii", "email"]
    sql: ${TABLE}.email ;;
  }

  dimension: contact_id {
    type: string
    label: "Contact ID"
    description: "HubSpot's contact ID (vid). Use to find the record in HubSpot."
    sql: ${TABLE}.contact_id ;;
  }

  dimension: first_name {
    type: string
    label: "First Name"
    description: "Contact's first name. Personal data."
    tags: ["pii"]
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    label: "Last Name"
    description: "Contact's last name. Personal data."
    tags: ["pii"]
    sql: ${TABLE}.last_name ;;
  }

  dimension: company {
    type: string
    label: "Company"
    description: "Company or organization on the contact record (for example a church or partner ministry)."
    sql: ${TABLE}.company ;;
  }

  dimension: email_domain {
    type: string
    label: "Email Domain"
    description: "The part of the email after @ (gmail.com, yahoo.com...). Useful for spotting deliverability problems with one mailbox provider."
    sql: SPLIT_PART(${email}, '@', 2) ;;
  }

  dimension_group: added {
    type: time
    label: "Contact Added"
    description: "When the contact was created in HubSpot, from HubSpot's create date (US Eastern)."
    timeframes: [date, week, month, quarter, year]
    convert_tz: no
    datatype: timestamp
    sql: ${TABLE}.added_at ;;
  }

  measure: contact_count {
    type: count
    label: "Contacts"
    description: "Number of unique contacts (one per email address). In the Contacts explore this is the total audience in HubSpot."
  }
}
