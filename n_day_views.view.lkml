view: n_day_views {
  derived_table: {
    sql: with

    play_data_global as
    (
    select * from allfirstplay.p0
    where user_id <> '0'
    and regexp_contains(user_id, r'^[0-9]*$')
    and user_id is not null
    and type = 'movie'
    -- and video_id = 3399212
    ),

    plays_most_granular as
    (
    select
    user_id
    , row_number() over (partition by user_id, date(timestamp), video_id order by date(timestamp)) as min_count
    , timestamp
    , collection
    , type
    , video_id
    , series
    , title
    , source
    , episode
    , email
    , winback
    from play_data_global
    order by
    user_id
    , date(timestamp)
    , video_id
    , min_count
    ),

    plays_max_duration as
    (
    select
    user_id
    , video_id
    , date(timestamp) as date
    , max(min_count) as min_count
    from plays_most_granular
    group by 1,2,3
    ),

    plays_less_granular as
    (
    select
    a.*
    , row_number() over (partition by a.user_id order by a.timestamp) as play_number
    from plays_most_granular as a
    inner join plays_max_duration as b
    on a.user_id = b.user_id
    and a.video_id = b.video_id
    and date(a.timestamp) = b.date
    and a.min_count = b.min_count
    ),

    movie_play_counts as
    (
    select
    collection
    , min(date(timestamp)) as dt_first_view
    , count(distinct user_id) as n
    from plays_less_granular
    group by 1
    ),

    n_day_views AS (
    select
    m.collection
    , m.dt_first_view
    , count(/*distinct*/ if(p.timestamp between timestamp(m.dt_first_view) and timestamp(m.dt_first_view) + interval 7 day, p.user_id, null)) as views_7_days
    , count(/*distinct*/ if(p.timestamp between timestamp(m.dt_first_view) and timestamp(m.dt_first_view) + interval 30 day, p.user_id, null)) as views_30_days
    , count(/*distinct*/ if(p.timestamp between timestamp(m.dt_first_view) and timestamp(m.dt_first_view) + interval 90 day, p.user_id, null)) as views_90_days
    from movie_play_counts as m
    inner join plays_less_granular as p
    on m.collection = p.collection
    group by 1, 2
    )

    select * from n_day_views ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: dt_first_view {
    type: date
    datatype: date
    sql: ${TABLE}.dt_first_view ;;
  }

  measure: views_7_days {
    type: sum
    sql: ${TABLE}.views_7_days ;;
    value_format_name: decimal_0
  }

  measure: views_30_days {
    type: sum
    sql: ${TABLE}.views_30_days ;;
    value_format_name: decimal_0
  }

  measure: views_90_days {
    type: sum
    sql: ${TABLE}.views_90_days ;;
    value_format_name: decimal_0
  }

  set: detail {
    fields: [
      collection,
      dt_first_view,
      views_7_days,
      views_30_days,
      views_90_days
    ]
  }
}
