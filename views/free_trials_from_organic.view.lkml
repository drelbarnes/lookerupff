# Distinct context_ip: Segment "organic social" UTM on pages, joined to order_completed (see docs/07).
# Templated dates bind to the dashboard "Date range" filter via free_trials_organic_date_range.
# In production, merge or include in upff.model.lkml.

view: free_trials_from_organic {
  label: "Free trials from organic (Segment)"

  derived_table: {
    sql: SELECT 1::int AS _anchor ;;
  }

  dimension: _anchor {
    hidden: yes
    primary_key: yes
    type: number
    sql: ${TABLE}._anchor ;;
  }

  # Listen target for dashboard: snapshot_date (maps here; drives liquid in the measure)
  filter: free_trials_organic_date_range {
    label: "Date range (page views / window)"
    type: date
    description: "Binds the organic pages timestamp window. Map dashboard filter Date range (snapshot) to this field in the UI if LookML listen does not support filter targets."
  }

  measure: organic_free_trial_ip_count {
    group_label: "Conversions"
    label: "Free trials from organic"
    type: number
    description: "COUNT(DISTINCT context_ip) on order_completed with prior organic-social UTM page hit (INNER join on anonymous_id OR context_ip). SQL matches docs/07 Segment section; event table is order_completed (swap LookML if you use a dedicated free_trial event)."
    value_format_name: decimal_0
    sql:
      (
        WITH
          a AS (
            SELECT DISTINCT
              m."timestamp" AS "timestamp",
              m.anonymous_id,
              m.context_ip
            FROM javascript_upff_home.pages m
            WHERE
              LOWER(TRIM(m.context_campaign_medium::varchar)) IN ('organic social', 'organic_social')
              AND m."timestamp"::date >=
                CASE
                  WHEN NULLIF(TRIM('{% date_start free_trials_organic_date_range %}'), '') IS NULL
                    THEN (CURRENT_DATE - 30)
                  ELSE
                    TO_DATE(NULLIF(TRIM('{% date_start free_trials_organic_date_range %}'), ''), 'YYYY-MM-DD')
                END
              AND m."timestamp"::date <
                CASE
                  WHEN NULLIF(TRIM('{% date_end free_trials_organic_date_range %}'), '') IS NULL
                    THEN (CURRENT_DATE + 1)
                  ELSE
                    (TO_DATE(NULLIF(TRIM('{% date_end free_trials_organic_date_range %}'), ''), 'YYYY-MM-DD') + 1)
                END
          ),
          ordered AS (
            SELECT DISTINCT
              anonymous_id,
              context_ip,
              "timestamp"
            FROM javascript_upentertainment_checkout.order_completed
          )
        SELECT
          COALESCE(
            COUNT(DISTINCT c.context_ip),
            0
          )::double precision
        FROM
          ordered c
          INNER JOIN a
            ON c.anonymous_id = a.anonymous_id
            OR c.context_ip = a.context_ip
      )
      ;;
  }
}
