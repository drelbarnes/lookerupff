view: bigquery_timeupdate_7day_vs_28day {
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
               case when sum(duration)=0 then null else coalesce(100.00*(COALESCE(SUM(timecode ), 0))/(COALESCE(SUM(duration ), 0)),0) end as percent_complete
        from timeupdate
        where date(timestamp) = date_sub(current_date(), interval 1 day)
        group by 1,2),

        days7 as
        (select title,
                collection,
               count(distinct user_id)/7 as avg_7_day_user_count,
               case when sum(duration)=0 then null else coalesce(100.00*(COALESCE(SUM(timecode ), 0))/(COALESCE(SUM(duration ), 0)),0) end as avg_7_day_percent_complete
        from timeupdate
        where date(timestamp) < date_sub(current_date(), interval 1 day) and date(timestamp) > date_sub(current_date(), interval 8 day)
        group by 1,2),

        days28 as
        (select title,
                collection,
               count(distinct user_id)/28 as avg_28_day_user_count,
               case when sum(duration)=0 then null else coalesce(100.00*(COALESCE(SUM(timecode ), 0))/(COALESCE(SUM(duration ), 0)),0) end as avg_28_day_percent_complete
        from timeupdate
        where date(timestamp) < date_sub(current_date(), interval 1 day) and date(timestamp) > date_sub(current_date(), interval 29 day)
        group by 1,2)

        select b1.*,
               coalesce(avg_7_day_user_count,0) as avg_7_day_user_count,
               coalesce(avg_7_day_percent_complete,0) as avg_7_day_percent_complete,
               coalesce(avg_28_day_user_count,0) as avg_28_day_user_count,
               coalesce(avg_28_day_percent_complete,0) as avg_28_day_percent_complete
        from currentday as b1 left join days7 as b2 on b1.title=b2.title and b1.collection=b2.collection
                              left join days28 as b3 on b1.title=b3.title and b1.collection=b3.collection
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
  }

  dimension: percent_complete {
    type: number
    sql: ${TABLE}.percent_complete ;;
    value_format: "0\%"
  }

  dimension: avg_7_day_user_count {
    type: number
    sql: ${TABLE}.avg_7_day_user_count ;;
    value_format: "0.0"
  }

  dimension: avg_7_day_percent_complete {
    type: number
    sql: ${TABLE}.avg_7_day_percent_complete ;;
    value_format: "0\%"
  }

  dimension: avg_28_day_user_count {
    type: number
    sql: ${TABLE}.avg_28_day_user_count ;;
    value_format: "0.0"
  }

  dimension: avg_28_day_percent_complete {
    type: number
    sql: ${TABLE}.avg_28_day_percent_complete ;;
    value_format: "0\%"
  }

  set: detail {
    fields: [
      title,
      collection,
      user_count,
      percent_complete,
      avg_7_day_user_count,
      avg_7_day_percent_complete,
      avg_28_day_user_count,
      avg_28_day_percent_complete
    ]
  }
}
