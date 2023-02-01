# The name of this view in Looker is "Statuses"
view: statuses {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: admin_bang_prod.statuses ;;
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
    sql: ${TABLE}.bango_user_id ;;
  }

  dimension: entitlement_id {
    type: string
    sql: ${TABLE}.entitlement_id ;;
  }

  dimension: immediate {
    type: yesno
    sql: ${TABLE}.immediate ;;
  }

  dimension: partner_id {
    type: number
    # hidden: yes
    sql: ${TABLE}.partner_id ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
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
