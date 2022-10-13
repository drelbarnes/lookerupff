view: wishlist_p1 {
    derived_table: {
      sql: with

              titles_p0 as (
              select distinct video_id, title,
              metadata_series_name as series,
              row_number() over (partition by video_id) as row,
              from php.get_titles
              ),

        titles_p1 as (select * from titles_p0 where row = 1),

        watchlist_p0 as (
        select user_id, video_id, context_traits_email,
        collection_title, video_title, context_page_path,
        date(timestamp) as date_stamp,
        row_number() over (partition by user_id order by timestamp) as event_num
        from javascript.added_to_watch_list_view
        where video_id is not null
        ),

        watchlist_p1 as (
        select a.*,
        coalesce(b.series, 'Movie') as series,
        coalesce(a.video_title, b.title) as title
        from watchlist_p0 as a
        left join titles_p1 as b
        on a.video_id = b.video_id),

        watchlist_p2 as (select user_id, max(event_num) as num_titles_in_watchlist from watchlist_p1 group by 1),

        watchlist_p3 as (select num_titles_in_watchlist, count(user_id) as number_of_users from watchlist_p2 group by 1),

        watchlist_p4 as (select title, series, count(user_id) as number_of_users from watchlist_p1 group by 1,2 order by 3 desc)

        select * from watchlist_p1
        ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: user_id {
      type: string
      sql: ${TABLE}.user_id ;;
    }

    dimension: video_id {
      type: number
      sql: ${TABLE}.video_id ;;
    }

    dimension: context_traits_email {
      type: string
      sql: ${TABLE}.context_traits_email ;;
    }

    dimension: collection_title {
      type: string
      sql: ${TABLE}.collection_title ;;
    }

    dimension: video_title {
      type: string
      sql: ${TABLE}.video_title ;;
    }

    dimension: context_page_path {
      type: string
      sql: ${TABLE}.context_page_path ;;
    }

    dimension: date_stamp {
      type: date
      datatype: date
      sql: ${TABLE}.date_stamp ;;
    }

    dimension: event_num {
      type: number
      sql: ${TABLE}.event_num ;;
    }

    dimension: series {
      type: string
      sql: ${TABLE}.series ;;
    }

    dimension: title {
      type: string
      sql: ${TABLE}.title ;;
    }

    set: detail {
      fields: [
        user_id,
        video_id,
        context_traits_email,
        collection_title,
        video_title,
        context_page_path,
        date_stamp,
        event_num,
        series,
        title
      ]
    }
  }
