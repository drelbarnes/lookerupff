view: bigquery_mvpd_titles {
    derived_table: {
      sql: with
              a as (
              select
                distinct platform
              from svod_titles.titles
              ),

              b as (
              select
                case
                  when platform = 'Comcast SVOD' then 'Comcast'
                  when platform not in ('Comcast SVOD','Roku','Amazon','Vimeo') then 'All Others'
                else platform end as platform2,
                case
                  when franchise not in ('Bringing Up Bates','Heartland') and content_type = 'Series' then 'Other Series'
                  when franchise = 'Bringing Up Bates' and content_type = 'Series' then 'Bringing Up Bates'
                  when franchise = 'Heartland' and content_type = 'Series' then 'Heartland'
                  when content_type = 'Movie' then 'Movies'
                else 'Null' end as content,
                views,
                content_type
              from svod_titles.titles
              )

              select * from b
               ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: platform2 {
      type: string
      sql: ${TABLE}.platform2 ;;
    }

    dimension: content {
      type: string
      sql: ${TABLE}.content ;;
    }

    dimension: views {
      type: number
      sql: ${TABLE}.views ;;
    }

    dimension: content_type {
      type: string
      sql: ${TABLE}.content_type ;;
    }

    measure: num_views {
      type: sum
      sql: ${TABLE}.views ;;
    }

    set: detail {
      fields: [platform2, content, views, content_type]
    }

  }
