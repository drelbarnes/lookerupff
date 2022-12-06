view: first_n_plays {
    derived_table: {
      sql:
      with

      play_data_global as
      (
      select * from allfirstplay.p0
      where user_id <> '0'
      and regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
      and type in ("{% parameter p_type %}")
      ),

      plays_most_granular as
      (
      select user_id,
      row_number() over (partition by user_id, date(timestamp), video_id order by date(timestamp)) as min_count,
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

      first_plays as
      (
      select collection, title, count(distinct user_id) as play_count
      from plays_less_granular
      where play_number = 1
      and timestamp between {% date_start date_filter %} and {% date_end date_filter %}
      group by 1,2
      ),

      second_plays as
      (
      select collection, title, count(distinct user_id) as play_count
      from plays_less_granular
      where play_number = 2
      and timestamp between {% date_start date_filter %} and {% date_end date_filter %}
      group by 1,2
      ),

      third_plays as
      (
      select collection, title, count(distinct user_id) as play_count
      from plays_less_granular
      where play_number = 3
      and timestamp between {% date_start date_filter %} and {% date_end date_filter %}
      group by 1,2
      ),

      fourth_plays as
      (
      select collection, title, count(distinct user_id) as play_count
      from plays_less_granular
      where play_number = 3
      and timestamp between {% date_start date_filter %} and {% date_end date_filter %}
      group by 1,2
      ),

      plays_first_four as
      (
      select collection, title, count(distinct user_id) as play_count
      from plays_less_granular
      where play_number in (1,2,3,4)
      and timestamp between {% date_start date_filter %} and {% date_end date_filter %}
      group by 1,2
      )

      select * from {% parameter table_name %} order by play_count desc
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

  parameter: p_type {
    label: "Type"
    type: unquoted

    allowed_value: {
      label: "Movies"
      value: "movie"
    }

    allowed_value: {
      label: "Series"
      value: "series"
    }

    allowed_value: {
      label: "Other"
      value: "other"
    }
  }

  parameter: table_name {
    label: "n-Play Depth"
    type: unquoted

    default_value: "plays_first_four"

    allowed_value: {
      label: "1st Play"
      value: "first_plays"
    }

    allowed_value: {
      label: "2nd Play"
      value: "second_plays"
    }

    allowed_value: {
      label: "3rd Play"
      value: "third_plays"
    }

    allowed_value: {
      label: "4th Play"
      value: "fourth_plays"
    }

    allowed_value: {
      label: "First Four Plays"
      value: "plays_first_four"
    }
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: play_count {
    type: number
    sql: ${TABLE}.play_count ;;
  }

  set: detail {
    fields: [collection, title, play_count]
  }
}
