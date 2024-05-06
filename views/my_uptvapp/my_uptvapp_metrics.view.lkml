view: my_uptvapp_metrics {
  derived_table: {
    sql:
      WITH unified_users AS (
        SELECT
          DATE(timestamp) AS event_date,
          COALESCE(user_id, anonymous_id) AS user_identifier
        FROM
          `my_uptv_dev.application_opened`
      ),
      daily_activity AS (
        SELECT
          event_date,
          user_identifier
        FROM
          unified_users
        GROUP BY
          event_date, user_identifier
      ),
      dau AS (
        SELECT
          event_date,
          COUNT(DISTINCT user_identifier) AS dau
        FROM
          daily_activity
        GROUP BY
          event_date
      ),
      new_users AS (
        SELECT
          user_identifier,
          MIN(event_date) AS first_event_date
        FROM
          daily_activity
        GROUP BY
          user_identifier
      ),
      new_users_daily AS (
        SELECT
          first_event_date AS event_date,
          COUNT(user_identifier) AS new_users
        FROM
          new_users
        GROUP BY
          first_event_date
      ),
      rolling_mau AS (
        SELECT
          a.event_date,
          COUNT(DISTINCT b.user_identifier) AS mau
        FROM
          (SELECT DISTINCT event_date FROM daily_activity) a
          CROSS JOIN daily_activity b
        WHERE
          b.event_date BETWEEN DATE_SUB(a.event_date, INTERVAL 29 DAY) AND a.event_date
        GROUP BY
          a.event_date
      ),
      combined AS (
        SELECT
          a.event_date,
          a.dau,
          r.mau,
          IFNULL(n.new_users, 0) AS new_users
        FROM
          dau a
          JOIN rolling_mau r USING (event_date)
          LEFT JOIN new_users_daily n ON a.event_date = n.event_date
      )
      SELECT
        event_date,
        dau,
        mau,
        new_users
      FROM
        combined
      ORDER BY
        event_date DESC
      ;;
  }

  dimension: event_date {
    type: date
    sql: ${TABLE}.event_date ;;
  }

  measure: dau {
    type: sum
    sql: ${TABLE}.dau ;;
  }

  measure: mau {
    type: sum
    sql: ${TABLE}.mau ;;
  }

  measure: new_users {
    type: sum
    sql: ${TABLE}.new_users ;;
  }

}
