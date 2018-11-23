view: bigquery_prior_days_title_performance {
  derived_table: {
    sql: with timeupdate as
      (with a1 as
      (select sent_at,
              user_id,
              (split(title," - ")) as title,
              a.current_time as _current_time
      from javascript.timeupdate as a),

      a2 as
      (select sent_at,
              user_id,
              title[safe_ordinal(1)] as title,
              _current_time
       from a1 order by 1),

       a3 as
      (select *
       from svod_titles.titles_id_mapping
       where (series is null and upper(collection)=upper(title)) or series is not null),

      a4 as
      ((SELECT
          a2.title,
          user_id,
          safe_cast(date(sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(_current_time) as timecode,
         'web' AS source
        FROM
          a2 inner join a3 on trim(upper(a2.title))=trim(upper(a3.title))
        WHERE
          user_id IS NOT NULL and safe_cast(user_id as string)!='0'
        GROUP BY 1,2,3,4)

      union all

      (SELECT
          title,
          user_id,
          safe_cast(date(sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'iOS' AS source
        FROM
          ios.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id
        WHERE
          user_id IS NOT NULL and safe_cast(user_id as string)!='0'
        GROUP BY 1,2,3,4)

        union all

      (SELECT
          title,
          user_id,
          safe_cast(date(sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Android' AS source
        FROM
          android.timeupdate as a inner join a3 on a.video_id=a3.id
        WHERE
          user_id IS NOT NULL and safe_cast(user_id as string)!='0'
        GROUP BY 1,2,3,4))

        select a4.*,
               collection,
               case when series is null and upper(collection)=upper(a3.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type
        from a4 inner join a3 on a4.title=a3.title),

        currentday as
        (select title,
                collection,
               count(distinct user_id) as user_count,
               coalesce(sum(timecode/3600),0) as hours_count,
               coalesce(sum(timecode/60),0) as minutes_count,
               coalesce(1.00*(COALESCE(SUM((timecode/60)), 0))/(COUNT(DISTINCT user_id )),0) as minutes_watched_per_user,
               coalesce(100.00*(COALESCE(SUM(timecode ), 0))/(COALESCE(SUM(duration ), 0)),0) as percent_complete
        from timeupdate
        where date(timestamp) = date_sub(current_date(), interval 1 day)
        group by 1,2),

        priorday as
        (select title,
                collection,
               count(distinct user_id) as prior_day_user_count,
               coalesce(sum(timecode/3600),0) as prior_day_hours_count,
               coalesce(sum(timecode/60),0) as prior_day_minutes_count,
               coalesce(1.00*(COALESCE(SUM((timecode/60)), 0))/(COUNT(DISTINCT user_id )),0) as prior_day_minutes_watched_per_user,
               coalesce(100.00*(COALESCE(SUM(timecode ), 0))/(COALESCE(SUM(duration ), 0)),0) as prior_day_percent_complete
        from timeupdate
        where date(timestamp) = date_sub(current_date(), interval 2 day)
        group by 1,2),

        priorweek as
        (select title,
                collection,
               count(distinct user_id) as prior_week_user_count,
               coalesce(sum(timecode/3600),0) as prior_week_hours_count,
               coalesce(sum(timecode/60),0) as prior_week_minutes_count,
               coalesce(1.00*(COALESCE(SUM((timecode/60)), 0))/(COUNT(DISTINCT user_id )),0) as prior_week_minutes_watched_per_user,
               coalesce(100.00*(COALESCE(SUM(timecode ), 0))/(COALESCE(SUM(duration ), 0)),0) as prior_week_percent_complete
        from timeupdate
        where date(timestamp) = date_sub(current_date(), interval 8 day)
        group by 1,2)

        select b1.*,
               coalesce(prior_day_user_count,0) as prior_day_user_count,
               coalesce(prior_day_hours_count,0) as prior_day_hours_count,
               coalesce(prior_day_minutes_count,0) as prior_day_minutes_count,
               coalesce(prior_day_minutes_watched_per_user,0) as prior_day_minutes_watched_per_user,
               coalesce(prior_day_percent_complete,0) as prior_day_percent_complete,
               coalesce(prior_week_user_count,0) as prior_week_user_count,
               coalesce(prior_week_hours_count,0) as prior_week_hours_count,
               coalesce(prior_week_minutes_count,0) as prior_week_minutes_count,
               coalesce(prior_week_minutes_watched_per_user,0) as prior_week_minutes_watched_per_user,
               coalesce(prior_week_percent_complete,0) as prior_week_percent_complete
        from currentday as b1 left join priorday as b2 on b1.title=b2.title and b1.collection=b2.collection
                              left join priorweek as b3 on b1.title=b3.title and b1.collection=b3.collection
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
    value_format: "0"
  }

  dimension: hours_count {
    type: number
    sql: ${TABLE}.hours_count ;;
    value_format: "0"
  }

  dimension: minutes_count {
    type: number
    sql: ${TABLE}.minutes_count ;;
    value_format: "0"
  }

  dimension: minutes_watched_per_user {
    type: number
    sql: ${TABLE}.minutes_watched_per_user ;;
    value_format: "0"
  }

  dimension: percent_complete {
    type: number
    sql: ${TABLE}.percent_complete ;;
    value_format: "0\%"
  }

  dimension: prior_day_user_count {
    type: number
    sql: ${TABLE}.prior_day_user_count ;;
    value_format: "0"
  }

  dimension: prior_day_hours_count {
    type: number
    sql: ${TABLE}.prior_day_hours_count ;;
    value_format: "0"
  }

  dimension: prior_day_minutes_count {
    type: number
    sql: ${TABLE}.prior_day_minutes_count ;;
    value_format: "0"
  }

  dimension: prior_day_minutes_watched_per_user {
    type: number
    sql: ${TABLE}.prior_day_minutes_watched_per_user ;;
    value_format: "0"
  }

  dimension: prior_day_percent_complete {
    type: number
    sql: ${TABLE}.prior_day_percent_complete ;;
    value_format: "0\%"
  }

  dimension: prior_week_user_count {
    type: number
    sql: ${TABLE}.prior_week_user_count ;;
    value_format: "0"
  }

  dimension: prior_week_hours_count {
    type: number
    sql: ${TABLE}.prior_week_hours_count ;;
    value_format: "0"
  }

  dimension: prior_week_minutes_count {
    type: number
    sql: ${TABLE}.prior_week_minutes_count ;;
    value_format: "0"
  }

  dimension: prior_week_minutes_watched_per_user {
    type: number
    sql: ${TABLE}.prior_week_minutes_watched_per_user ;;
    value_format: "0"
  }

  dimension: prior_week_percent_complete {
    type: number
    sql: ${TABLE}.prior_week_percent_complete ;;
    value_format: "0\%"
  }

  set: detail {
    fields: [
      title,
      collection,
      user_count,
      hours_count,
      minutes_count,
      minutes_watched_per_user,
      percent_complete,
      prior_day_user_count,
      prior_day_hours_count,
      prior_day_minutes_count,
      prior_day_minutes_watched_per_user,
      prior_day_percent_complete,
      prior_week_user_count,
      prior_week_hours_count,
      prior_week_minutes_count,
      prior_week_minutes_watched_per_user,
      prior_week_percent_complete
    ]
  }
}
