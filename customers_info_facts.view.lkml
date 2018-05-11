view: customers_info_facts {
  derived_table: {
    explore_source: customers {
      limit: 10000
      column: customer_id {
        field: customers.customer_id
      }

      column: status {
        field: customers.status
      }
    }
  }
  dimension: customer_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.customer_id ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }
 }
