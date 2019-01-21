view: bigquery_free_to_paid {
  derived_table: {
    sql: SELECT p.user_id, p.email, u.context_campaign_name, p.received_at, topic, created_at,
              "Web" as os
          FROM javascript.users AS u, http_api.purchase_event AS p
            WHERE u.email = p.email

      union all
      select u.id AS user_id, p.email, i.context_campaign_name, p.received_at, topic, created_at,
             "Android" as os
      from android.branch_install AS i, android.users AS u, http_api.purchase_event AS p WHERE i.anonymous_id = u.context_traits_anonymous_id AND u.id = p.user_id

      union all
      select u.id AS user_id, p.email, i.context_campaign_name, p.received_at, topic, created_at,
             "Android" as os
      from android.branch_reinstall AS i, android.users AS u, http_api.purchase_event AS p WHERE i.anonymous_id = u.context_traits_anonymous_id AND u.id = p.user_id

      union all
      select u.id AS user_id, p.email, context_campaign_name, p.received_at, topic, created_at,
             "iOS" as os
      from ios.branch_install AS i, ios.users AS u, http_api.purchase_event AS p WHERE i.context_device_id = u.context_device_id AND u.id = p.user_id

      union all
      select u.id AS user_id, p.email, context_campaign_name, p.received_at, topic, created_at,
             "iOS" as os
      from ios.branch_reinstall AS i, ios.users AS u, http_api.purchase_event AS p WHERE i.context_device_id = u.context_device_id AND u.id = p.user_id


      ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: context_campaign_name {
    type: string
    sql: ${TABLE}.context_campaign_name ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension_group: created {
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
    sql: ${TABLE}.created_at ;;
  }

 measure: count {
    type: count_distinct
    sql: ${email} ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: os {
    type: string
    sql: ${TABLE}.os ;;
  }


}
