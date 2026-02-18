view: google_organic_search {
  derived_table: {
    sql:
    with orig as (
      SELECT
        data_date
        ,lower(query) as original_query
      FROM `up-faith-and-family-216419.searchconsole.searchdata_site_impression`
      ),

    transform AS (
     SELECT
      data_date as report_date,
      original_query,
      CASE
        WHEN LOWER(original_query) LIKE '%up%faith%and%family%'
          OR LOWER(original_query) LIKE '%up%faith%&%family%'
          THEN 'upfaithandfamily'

        WHEN LOWER(original_query) LIKE '%up family%'
          OR LOWER(original_query) LIKE '%upfamily%'
          THEN 'upfamily'

        WHEN LOWER(original_query) LIKE '%upff%'
          OR LOWER(original_query) LIKE '%up ff%'
          THEN 'upff'

        WHEN LOWER(original_query) LIKE '%up tv%'
          OR LOWER(original_query) LIKE '%uptv%'
          THEN 'uptv'

        WHEN LOWER(original_query) LIKE '%heartland%'
          OR LOWER(original_query) LIKE '%heart land%'
          THEN 'heartland'

        WHEN LOWER(original_query) LIKE '%hudson and rex%'
          OR LOWER(original_query) LIKE '%hudson%&%rex%'
          OR LOWER(original_query) LIKE '%hudson rex%'
          THEN 'hudsonandrex'

        WHEN LOWER(original_query) LIKE '%blue skies%'
          OR LOWER(original_query) LIKE '%blueskies%'
          THEN 'blueskies'
        ELSE original_query
      END AS final_query
      FROM orig
      )

      SELECT
        final_query
        ,report_date
        ,COUNT(*) AS query_count
      FROM transform
      group by 1,2
      ;;
  }
  dimension: date {
    type: date
    sql:  ${TABLE}.report_date ;;
  }

  dimension: query_count {
    type: number
    sql:  ${TABLE}.query_count ;;
  }
  dimension: final_query {
    type: string
    sql:  ${TABLE}.final_query ;;
  }
  measure: count {
    type: sum
    sql: ${TABLE}.query_count ;;
  }
}
