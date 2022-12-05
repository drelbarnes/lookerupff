view: user_play_history {
    derived_table: {
      sql: with

              play_data_global as
              (
              select * from allfirstplay.p0
              where user_id <> '0'
              and regexp_contains(user_id, r'^[0-9]*$')
              and user_id is not null
              ),

        plays_most_granular as
        (
        select user_id,
        row_number() over (partition by user_id, date(timestamp),
        video_id order by date(timestamp)) as min_count,
        timestamp, collection, type, video_id, series,
        title, source, episode, email, winback
        from play_data_global
        order by user_id,
        date(timestamp), video_id, min_count
        ),

        plays_max_duration as
        (
        select user_id, video_id,
        date(timestamp) as date,
        max(min_count) as min_count
        from plays_most_granular
        group by 1,2,3
        ),

        plays_less_granular as
        (
        select a.*, row_number() over (partition by a.user_id order by a.timestamp) as play_number
        from plays_most_granular as a
        inner join plays_max_duration as b
        on a.user_id = b.user_id
        and a.video_id = b.video_id
        and date(a.timestamp) = b.date
        and a.min_count = b.min_count
        ),

        views_in_last_14_days as
        (
        select distinct user_id
        from plays_less_granular
        where date(timestamp) between current_date()-28 and current_date()
        )

        select user_id, date(timestamp) as date_stamp, type, video_id,
        series, collection, title, episode, source, email, play_number, min_count
        from plays_less_granular
        order by user_id, play_number

        -- select * from plays_less_granular order by user_id, play_number limit 5000
        ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    measure: min_count {
      type: number
      sql: ${TABLE}.min_count ;;
    }

    dimension: user_id {
      type: string
      sql: ${TABLE}.user_id ;;
    }

    dimension: date_stamp {
      type: date
      datatype: date
      sql: ${TABLE}.date_stamp ;;
    }

    dimension: type {
      type: string
      sql: ${TABLE}.type ;;
    }

    dimension: video_id {
      type: number
      sql: ${TABLE}.video_id ;;
    }

    dimension: series {
      type: string
      sql: ${TABLE}.series ;;
    }

    dimension: collection {
      type: string
      sql: ${TABLE}.collection ;;
    }

    dimension: title {
      type: string
      sql: ${TABLE}.title ;;
    }

    dimension: episode {
      type: number
      sql: ${TABLE}.episode ;;
    }

    dimension: source {
      type: string
      sql: ${TABLE}.source ;;
    }

    dimension: email {
      type: string
      sql: ${TABLE}.email ;;
    }

    dimension: play_number {
      type: number
      sql: ${TABLE}.play_number ;;
    }

    set: detail {
      fields: [
        user_id,
        date_stamp,
        type,
        video_id,
        series,
        collection,
        title,
        episode,
        source,
        email,
        play_number
      ]
    }
  }
