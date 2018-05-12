view: customers_info_facts {
  derived_table: {
    explore_source: customers {
      limit: 33000
      column: customer_id {
        field: customers.customer_id
      }

      column: status {
        field: customers.status
      }

      column: email {
        field: customers.email
      }

      column: platform {
        field: customers.platform
      }
    }
  }
  dimension: customer_id {
    type: number
    tags: ["user_id"]
    primary_key: yes
    sql: ${TABLE}.customer_id ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }
 }
