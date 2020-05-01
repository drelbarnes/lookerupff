view: bigquery_timeupdate {
  derived_table: {
    sql:with a30 as
(select video_id,
       max(ingest_at) as ingest_at
from php.get_titles
group by 1),

a3 as
(select distinct
       metadata_series_name as series,
       case when metadata_season_name in ('Season 1','Season 2','Season 3') then concat(metadata_series_name,'-',metadata_season_name)
            when metadata_season_name is null then metadata_movie_name
            else metadata_season_name end as collection,
       season_number as season,
       a.title,
       a.video_id as id,
       episode_number as episode,
       date(time_available) as date,
       round(duration_seconds/60) as duration,
       promotion
from php.get_titles as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id inner join a30 on a30.video_id=a.video_id and a30.ingest_at=a.ingest_at
 where date(a.loaded_at)>='2020-02-13'  ),

a31 as
(select mysql_roku_firstplays_firstplay_date_date as timestamp,
                mysql_roku_firstplays_video_id,
                user_id,
                max(loaded_at) as maxloaded
from looker.roku_firstplays
group by 1,2,3),

a32 as
(select a31.timestamp,
       a31.mysql_roku_firstplays_video_id,
       a31.user_id,
       count(*) as numcount,
       sum(mysql_roku_firstplays_total_minutes_watched) as mysql_roku_firstplays_total_minutes_watched
from looker.roku_firstplays as a inner join a31 on a.loaded_at=maxloaded and mysql_roku_firstplays_firstplay_date_date=a31.timestamp and a31.mysql_roku_firstplays_video_id=a.mysql_roku_firstplays_video_id and a.user_id=a31.user_id
group by 1,2,3),

a4 as
((SELECT
    a3.title,
    a.user_id,
    email,
    cast(video_id as int64) as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(a3.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(a.sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Web' AS source
  FROM
    javascript.durationchange as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
  WHERE
    a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

union all

(SELECT
    title,
    a.user_id,
    email,
    cast(video_id as int64) as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(a.sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'iOS' AS source
  FROM
    ios.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
  WHERE
    a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

  union all

  (SELECT
    title,
    a.user_id,
    email,
    cast(video_id as int64) as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(a.sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'iOS' AS source
  FROM
    ios.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
  WHERE
    a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

  union all

  (SELECT
    title,
    a.user_id,
    email,
    cast(video_id as int64) as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(a.sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Roku' AS source
  FROM
    roku.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
  WHERE
    a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

  union all

  (SELECT
    title,
    a.user_id,
    email,
    cast(video_id as int64) as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(a.sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Android' AS source
  FROM
    android.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
  WHERE
    a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

  union all

  (SELECT
    a3.title,
    a.user_id,
    email,
    cast(video_id as int64) as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(a3.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(a.sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Web' AS source
  FROM
    javascript.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
  WHERE
    a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

  union all

  (SELECT
    title,
    a.user_id,
    email,
    cast(video_id as int64) as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(a.sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Roku' AS source
  FROM
    roku.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
  WHERE
    a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

  union all

(SELECT
    title,
    a.user_id,
    email,
    video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(a.sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Android' AS source
  FROM
    android.timeupdate as a inner join a3 on a.video_id=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
  WHERE
    a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

  union all

  (SELECT
    distinct
    a3.title,
    a.user_id,
    email,
     mysql_roku_firstplays_video_id as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    a.timestamp,
    a3.duration*60 as duration,
    mysql_roku_firstplays_total_minutes_watched*60 as timecode,
   'Roku' AS source
  FROM
    a32 as a inner join a3 on  mysql_roku_firstplays_video_id=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
  WHERE
    a.user_id IS NOT NULL /*and a.user_id<>'0'*/ and a3.duration>0))

  select *,
       case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
            else "NA"
            end as Quarter
from a4 as a;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: Quarter {
    type: string
    sql: ${TABLE}.quarter ;;
  }

  dimension: current_date {
    type: date
    sql: current_date() ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: series {
    type: string
    sql: ${TABLE}.series ;;
  }

  dimension: season {
    type: string
    sql: ${TABLE}.season ;;
  }

  dimension: episode {
    type: number
    sql: ${TABLE}.episode ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }


  dimension: timecode {
    type: number
    sql: ${TABLE}.timecode  ;;
  }

  dimension: duration {
    type: number
    sql: ${TABLE}.duration ;;
  }

  dimension: hours_watched {
    type: number
    sql: ${timecode}/3600 ;;
    value_format: "#,##0"
  }

  dimension: minutes_watched {
    type: number
    sql: case when ${duration}< ${timecode} then round(${duration}/60) else round(${timecode}/60) end ;;
    value_format: "#,##0"
  }

  measure: title_count {
    type: count_distinct
    sql: ${video_id} ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: kids_genre {
    type: string
    sql: case when ${collection} LIKE '%Owlegories%' OR ${collection} LIKE '%Abe & Bruno%' OR ${collection} LIKE '%Seventeen Again%' OR ${collection} LIKE '%Albert: Up, Up and Away!%' OR ${collection} LIKE '%Spirit Bear%' OR ${collection} LIKE '%The Great Mom Swap%' OR ${collection} LIKE '%Gadgetman%' OR ${collection} LIKE '%Zoo Juniors%' OR ${collection} LIKE '%Angels in Training%' OR ${collection} LIKE '%Treasure State%' OR ${collection} LIKE '%The Big Comfy Couch%' OR ${collection} LIKE '%Undercover Kids%' OR ${collection} LIKE '%On the Wings of the Monarch%' OR ${collection} LIKE '%Learning To See: The World of Insects%' OR ${collection} LIKE '%Trailer Made%' OR ${collection} LIKE '%Creeping Things%' OR ${collection} LIKE '%Crimes and Mister Meanors%' OR ${collection} LIKE '%Out of the Wilderness%' OR ${collection} LIKE '%Lost Wilderness%' OR ${collection} LIKE '%The Fix It Boys%' OR ${collection} LIKE '%Gibby%' OR ${collection} LIKE '%Flood Geology Series%' OR ${collection} LIKE '%The Prince and the Pauper%' OR ${collection} LIKE '%Our House: The Puzzle Maker%' OR ${collection} LIKE '%The Torchlighters%' OR ${collection} LIKE '%Meow Manor%' OR ${collection} LIKE '%The Lion of Judah%' OR ${collection} LIKE '%Testament: The Bible in Animation%' OR ${collection} LIKE '%The Passion: A Brickfilm%' OR ${collection} LIKE '%Horse Crazy%' OR ${collection} LIKE '%Kid Cop%' OR ${collection} LIKE '%The Sandman and the Lost Sand of Dreams%' OR ${collection} LIKE '%The Saddle Club%' OR ${collection} LIKE '%Junior\'s Giants%' OR ${collection} LIKE '%Genesis 7%' OR ${collection} LIKE '%The Lost Medallion: The Adventures of Billy Stone%' OR ${collection} LIKE '%Davey & Goliath%' OR ${collection} LIKE '%Legend of The Lost Tomb%' OR ${collection} LIKE '%My Dad\'s a Soccer Mom%' OR ${collection} LIKE '%Little Heroes%' OR ${collection} LIKE '%Ms. Bear%' OR ${collection} LIKE '%Whiskers%' OR ${collection} LIKE '%The Country Mouse and the City Mouse Adventures%' OR ${collection} LIKE '%Monkey Business%' OR ${collection} LIKE '%The Sugar Creek Gang%' OR ${collection} LIKE '%Awesome Science%' OR ${collection} LIKE '%The Ghost Club%' OR ${collection} LIKE '%Animals are People Too!%' OR ${collection} LIKE '%Who\'s Watching the Kids%' OR ${collection} LIKE '%Touched by Grace%' OR ${collection} LIKE '%Mandie And The Cherokee Treasure%' OR ${collection} LIKE '%Mandie and the Secret Tunnel%' OR ${collection} LIKE '%Finding Buck McHenry%' OR ${collection} LIKE '%The Princess Stallion%' OR ${collection} LIKE '%Adventures of Chris Fable%' OR ${collection} LIKE '%The Wild Stallion%' OR ${collection} LIKE '%A Horse Called Bear%' OR ${collection} LIKE '%The Flizbins: Cowboys & Bananas%' OR ${collection} LIKE '%Jacob on the Road%' OR ${collection} LIKE '%Cedarmont Kids%' OR ${collection} LIKE '%Kingdom Under the Sea%' OR ${collection} LIKE '%Uncle Nino%'
         then 'Kids' else 'Non-Kids' end;;
        }


  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      day_of_week_index,
      day_of_week,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }


  measure: count {
    type: count
    drill_fields: [detail*]
  }


  measure: duration_count {
    type: sum
    sql: ${duration} ;;
  }

  measure: percent_completed {
    type: number
    value_format: "0\%"
    sql: case when ${timecode_count}>${duration_count} then 100.00 else 100.00*${timecode_count}/${duration_count} end ;;
  }

  measure: timecode_count {
    type: sum
    value_format: "0"
    sql: ${timecode} ;;
  }

  measure: hours_count {
    type: sum
    value_format: "#,##0"
    sql: ${hours_watched};;
  }

  measure: minutes_count {
    type: sum
    value_format: "#,##0"
    sql: ${minutes_watched};;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: hours_watched_per_user {
    type: number
    sql: 1.0*${hours_count}/${user_count} ;;
    value_format: "0.0"
  }

  measure: minutes_watched_per_user {
    type: number
    sql: 1.00*${minutes_count}/${user_count} ;;
    value_format: "0"
  }

## filter determining time range for all "A" measures
  filter: time_a {
    type: date_time
  }

## flag for "A" measures to only include appropriate time range
  dimension: group_a {
    hidden: no
    type: yesno
    sql: {% condition time_a %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  measure: hours_a {
    type: sum
    filters: {
      field: group_a
      value: "yes"
    }
    sql: ${hours_watched} ;;
    value_format: "#,##0"
  }

## filter determining time range for all "b" measures
  filter: time_b {
    type: date_time
  }

## flag for "B" measures to only include appropriate time range
  dimension: group_b {
    hidden: no
    type: yesno
    sql: {% condition time_b %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  filter: minutes_a {
    type: number
  }

  dimension: sample_a {
    hidden: no
    type: number
    sql: {% condition minutes_a %} ${minutes_watched} {% endcondition %};;
  }

  measure: completion_rate_a {
    type: sum
    filters: {
      field: minutes_a
      value: ">5"
    }
    sql: 100.00*${timecode}/${duration} ;;
  }

  measure: hours_b {
    type: sum
    filters: {
      field: group_b
      value: "yes"
    }
    sql: ${hours_watched} ;;
    value_format: "#,##0"
  }

  measure: user_count_a {
    type: count_distinct
    filters: {
      field: group_a
      value: "yes"
    }
    sql: ${user_id}  ;;
    value_format: "#,##0"
  }

  measure: user_count_b {
    type: count_distinct
    filters: {
      field: group_b
      value: "yes"
    }
    sql: ${user_id} ;;
    value_format: "#,##0"
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }
}
