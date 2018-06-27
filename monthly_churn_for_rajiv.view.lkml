view: monthly_churn_for_rajiv {
  derived_table: {
    sql: with a as

      (select cast(datepart(month,date(a.timestamp)) as varchar) as month,
             ROW_NUMBER() OVER(ORDER BY cast(a.timestamp as varchar) desc) AS Row,
             total_paying
      from customers.analytics as a),

      b as

      (select month,
             max(Row) as MaxRow
      from a
      group by month),

      c as

      (select a.month,
             total_paying
      from a inner join b on Row=MaxRow),

      d as

      (select cast(datepart(month,date(a.timestamp)) as varchar) as month,
            sum(paying_churn) as total_churn
      from customers.analytics as a
      group by cast(datepart(month,date(a.timestamp)) as varchar))

      select c.*,
             total_churn,
             cast(total_churn as decimal)/cast(total_paying as decimal) as churn_percent
      from c inner join d on c.month=d.month
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: month {
    type: string
    sql: ${TABLE}.month ;;
  }

  dimension: total_paying {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  dimension: total_churn {
    type: number
    sql: ${TABLE}.total_churn ;;
  }

  dimension: churn_percent {
    type: number
    sql: ${TABLE}.churn_percent ;;
  }

  set: detail {
    fields: [month, total_paying, total_churn, churn_percent]
  }
}
