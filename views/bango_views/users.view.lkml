# The name of this view in Looker is "Users"
view: users {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: admin_bang_prod.users ;;
  drill_fields: [id]
  # This primary key is the unique key for this table in the underlying database.
  # You need to define a primary key in a view in order to join to other views.

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  # Here's what a typical dimension looks like in LookML.
  # A dimension is a groupable field that can be used to filter query results.
  # This dimension will be called "Bango User ID" in Explore.

  dimension: bango_user_id {
    type: number
    sql: ${TABLE}.bango_user_Id ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: ${TABLE}.customer_id ;;
  }

  dimension_group: date_activated {
    type: time
    sql: ${TABLE}.date_activated ;;
  }

  dimension_group: date_created {
    type: time
    sql: ${TABLE}.date_created ;;
  }

  dimension_group: date_ended {
    type: time
    sql: ${TABLE}.date_ended ;;
  }

  dimension_group: date_resumed {
    type: time
    sql: ${TABLE}.date_resumed ;;
  }

  dimension_group: date_suspended {
    type: time
    sql: ${TABLE}.date_suspended ;;
  }

  dimension: email_hashed {
    type: string
    sql: ${TABLE}.email_hashed ;;
  }

  dimension: entitlement_id {
    type: string
    sql: ${TABLE}.entitlement_id ;;
  }

  dimension: offer_key {
    type: string
    sql: ${TABLE}.offer_key ;;
  }

  dimension: partner_id {
    type: number
    # hidden: yes
    sql: ${TABLE}.partner_id ;;
  }

  dimension: product_key {
    type: string
    sql: ${TABLE}.product_key ;;
  }

  dimension: reseller_key {
    type: string
    sql: ${TABLE}.reseller_key ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  measure: count {
    type: count
    drill_fields: [id, partners.id]
  }
}
