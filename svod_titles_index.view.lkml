view: svod_titles_index {
    derived_table: {
      sql: with

              titles as
              (
              select
                *,
                case
                  when platform = 'Comcast SVOD' then 'Comcast'
                  when platform in ('DISH', 'Sling') then 'DISH'
                else platform end as platform1
              from svod_titles.titles
              where year is not null
              ),

              subs as
              (
              select
                *
              from svod_titles.mvpd_subs
              where year is not null
              ),

              contribution_p0 as
              (
              select
                year,
                month,
                amazon,
                d2c,
                roku,
                comcast,
                cox,
                directv,
                dish_sling,
                appletv,
                youtube,
                amazon+comcast+cox+directv+dish_sling+roku+appletv+youtube+d2c as total
              from subs
              ),

              contribution_p1 as
              (
              select
                year,
                month,
                safe_cast(total as int64) as Total,
                round(amazon/total,3) as Amazon,
                round(d2c/total,3) as Vimeo,
                round(roku/total,3) as Roku,
                round(comcast/total,3) as Comcast,
                round(cox/total,3) as Cox,
                round(directv/total,3) as DirecTV,
                round(dish_sling/total,3) as DISH,
                round(appletv/total,3) as Apple,
                round(youtube/total,3) as YouTube
              from contribution_p0
              ),

              contribution_p2 as
              (
              select * from contribution_p1
              unpivot
                (
                expected for platform in
                  (
                  Amazon,
                  Vimeo,
                  Roku,
                  Comcast,
                  Cox,
                  DirecTV,
                  DISH,
                  Apple,
                  YouTube
                  )
                )
              ),

              index_p0 as
              (
              select
                year,
                month,
                up_title,
                sum(views) as total_views
              from titles
              group by 1,2,3
              ),

              index_p1 as
              (
              select
                a.year,
                a.month,
                a.platform1 as platform,
                a.up_title,
                a.content_type,
                b.total_views,
                sum(a.views) as sum_views
              from titles as a
              left join index_p0 as b
              on a.year=b.year
              and a.month=b.month
              and a.up_title=b.up_title
              group by 1,2,3,4,5,6
              ),

              index_p2 as
              (
              select
                a.*,
                round(safe_divide(a.sum_views,a.total_views),3) as p_views,
                b.expected as e_views,
                round(safe_divide(safe_divide(a.sum_views,a.total_views)-b.expected,b.expected),3) as index
              from index_p1 as a
              left join contribution_p2 as b
              on a.year=b.year
              and a.month=b.month
              and a.platform=b.platform
              )

              select * from index_p2 where content_type = 'Movie' ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: year {
      type: number
      sql: ${TABLE}.year ;;
    }

    dimension: month {
      type: number
      sql: ${TABLE}.month ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: up_title {
      type: string
      sql: ${TABLE}.up_title ;;
    }

    dimension: content_type {
      type: string
      sql: ${TABLE}.content_type ;;
    }

    measure: total_views {
      type: sum
      value_format: "#,##0"
      sql: ${TABLE}.total_views ;;
    }

    measure: sum_views {
      type: sum
      value_format: "#,##0"
      sql: ${TABLE}.sum_views ;;
    }

    measure: p_views {
      type: sum
      value_format: "0.00%"
      sql: ${TABLE}.p_views ;;
    }

    measure: e_views {
      type: sum
      value_format: "0.00%"
      sql: ${TABLE}.e_views ;;
    }

    measure: index {
      type: sum
      value_format: "0"
      sql: ${TABLE}.index ;;
    }

    set: detail {
      fields: [
        year,
        month,
        platform,
        up_title,
        content_type,
        total_views,
        sum_views,
        p_views,
        e_views,
        index
      ]
    }
  }
