view: popular_content_by_series {
  derived_table: {
    sql:
      with

      play_data_global as
      (
      select
      *
      from allfirstplay.p0
      where user_id <> '0'
      and regexp_contains(user_id, r'^[0-9]*$')
      and date(timestamp) between {% date_start date_filter %} and {% date_end date_filter %}
      ),

      plays_most_granular as
      (
      select
      user_id,
      row_number() over (partition by user_id, date(timestamp), video_id order by date(timestamp)) as min_count,
      timestamp,
      collection,
      type,
      video_id,
      series,
      title,
      source,
      episode,
      email,
      winback
      from play_data_global
      order by
      user_id,
      date(timestamp),
      video_id,
      min_count
      ),

      plays_max_duration as
      (
      select
      user_id,
      video_id,
      date(timestamp) as date,
      max(min_count) as min_count
      from plays_most_granular
      group by 1,2,3
      ),

      plays_less_granular as
      (
      select
      a.*,
      row_number() over (partition by a.user_id order by a.timestamp) as play_number
      from plays_most_granular as a
      inner join plays_max_duration as b
      on a.user_id = b.user_id
      and a.video_id = b.video_id
      and date(a.timestamp) = b.date
      and a.min_count = b.min_count
      ),

      audience_select_p0 as
      (
      select
      user_id,
      min_count,
      timestamp,
      title,
      source,
      episode
      from plays_less_granular
      where collection = {% parameter c_name %}
      and min_count > 10
      ),

      audience_select_p1 as
      (
      select
      *,
      row_number() over (partition by user_id order by timestamp) as play_nbr
      from audience_select_p0
      ),

      audience_select_p2 as
      (
      select
      user_id,
      max(play_nbr) as max_plays
      from audience_select_p1
      group by user_id
      ),

      audience_select_p3 as
      (
      select
      *
      from audience_select_p2
      where max_plays > 2
      ),

      title_analysis_p0 as
      (
      select
      a.*,
      b.max_plays
      from plays_less_granular as a
      right join audience_select_p3 as b
      on a.user_id = b.user_id
      ),

      movies_p0 as
      (
      select
      title as collection,
      count(user_id) as num_plays
      from title_analysis_p0
      where type <> 'series'
      and title is not null
      group by title
      ),

      series_p0 as
      (
      select
      collection,
      count(user_id) as num_plays
      from title_analysis_p0
      where type = 'series'
      and collection <> {% parameter c_name %}
      and title is not null
      group by collection
      )

      select * from {% parameter p_type %} order by num_plays desc
      ;;
  }

  filter: date_filter {
    label: "Date Range"
    type: date
  }

  filter: end_date {
    label: "End Date"
    type: date
  }

  parameter: c_name {
    label: "Type"
    type: unquoted
  }

  parameter: p_type {
    type: unquoted
    label: "Type"
    allowed_value: {label: "Movies" value: "movies_p0"}
    allowed_value: {label: "Series" value: "series_p0"}
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: num_plays {
    type: number
    sql: ${TABLE}.num_plays ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  set: detail {
    fields: [collection, num_plays]
  }
}
