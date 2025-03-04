view: minutes_distribution_by_title {
    derived_table: {
      sql: with

              play_data_global as
              (
              select * from allfirstplay.p0
              where user_id <> '0'
              and regexp_contains(user_id, r'^[0-9]*$')
              and user_id is not null
              and type = 'movie'
              --and video_id = 3399212
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

              title_durations as
              (
              select distinct
                  title as collection
                , round(duration_seconds / 60, 0) as duration_mins
              from php.get_titles
              ),

              minutes_by_title as
              (
              select
                  a.collection
                , a.min_count
                , count(a.user_id) as number_viewers
              from plays_less_granular as a
              left join title_durations as b
              on a.collection = b.collection
              where a.min_count < b.duration_mins + 1
              group by 1,2
              )

              select
                  collection
                , min_count
                , number_viewers
                , number_viewers * 100.0 / sum(number_viewers) over (partition by collection) as percent_of_total
                , sum(number_viewers) over (partition by collection order by min_count) * 100.0 / sum(number_viewers) over (partition by collection) as cumulative
                , (1 - sum(number_viewers) over (partition by collection order by min_count) * 1.0 / sum(number_viewers) over (partition by collection)) * 100 as reverse_cumulative
              from minutes_by_title
              where number_viewers > 2000
              order by collection, min_count
              ;;
    }

  measure: reverse_cumulative_measure {
    type: number
    sql: round(max(${reverse_cumulative}), 0) ;;
    value_format: "0"
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: min_count {
    type: number
    sql: ${TABLE}.min_count ;;
  }

  dimension: number_viewers {
    type: number
    sql: ${TABLE}.number_viewers ;;
  }

  dimension: percent_of_total {
    type: number
    sql: ${TABLE}.percent_of_total ;;
  }

  dimension: cumulative {
    type: number
    sql: ${TABLE}.cumulative ;;
  }

  dimension: reverse_cumulative {
    type: number
    sql: ${TABLE}.reverse_cumulative ;;
  }

  set: detail {
    fields: [
      collection,
      min_count,
      number_viewers,
      percent_of_total,
      cumulative,
      reverse_cumulative
    ]
  }
}
