view: hudson_rex_s2_e19 {
  derived_table: {
    sql: with

            /* pulls all churned customers by status_date */
            aa as
            (
            select
              user_id,
              email,
              status_date as churn_date
            from http_api.purchase_event
            where topic in ('customer.product.cancelled','customer.product.disabled','customer.product.expired')
            ),

      /* pulls all winback customers by status_date */
      bb as
      (
      select
      user_id,
      email,
      max(status_date) as status_date
      from http_api.purchase_event
      where topic in ('customer.product.created','customer.product.renewed','customer.created','customer.product.free_trial_created')
      group by 1,2
      ),

      /* creates table of reacquired customers */
      cc as
      (
      select
      distinct bb.user_id,
      bb.email
      from aa inner join bb
      on aa.user_id = bb.user_id
      and status_date > churn_date
      ),

      /* for older dates, we leverage firstplay tables */
      a1 as
      (
      select
      sent_at as timestamp,
      user_id, (split(title," - ")) as title
      from javascript.firstplay
      ),

      /* formats title metadata */
      a2 as
      (
      select
      timestamp,
      user_id,
      title[safe_ordinal(1)] as title,
      concat(title[safe_ordinal(2)]," - ",title[safe_ordinal(3)]) as collection
      from a1
      order by 1
      ),

      /* use php.get_titles table to create title id mapping table that maps video id to title of any given asset */
      a30 as
      (
      select
      video_id,
      max(loaded_at) as loaded_at
      from php.get_titles
      group by 1
      ),

      /* formats title metadata */
      titles_id_mapping as
      (
      select
      distinct metadata_series_name as series,
      case
      when metadata_season_name in ('Season 1','Season 2','Season 3') then concat(metadata_series_name,'-',metadata_season_name)
      when metadata_season_name is null then metadata_movie_name
      else metadata_season_name
      end as collection,
      season_number as season,
      a.title,
      a.video_id as id,
      episode_number as episode,
      date(time_available) as date,
      date(time_unavailable) as end_date,
      round(duration_seconds/60) as duration,
      promotion
      from php.get_titles as a
      left join svod_titles.titles_id_mapping as b
      on a.video_id = b.id
      inner join a30
      on a30.video_id = a.video_id
      and a30.loaded_at = a.loaded_at
      ),

      /* call legacy roku firstplay table for old dates */
      a32 as
      (
      select
      distinct mysql_roku_firstplays_firstplay_date_date as timestamp,
      mysql_roku_firstplays_video_id,
      user_id,
      '' as anonymousId,
      'firstplay' as event_type,
      UNIX_SECONDS(mysql_roku_firstplays_firstplay_date_date) as EPOCH_TIMESTAMP,
      CAST('1111' AS int64) as platform_id
      from looker.roku_firstplays),

      /* build master dataset for engagement using
      firstplay tables for older dates and the current
      video_content_playing tables for current engagement
      ingestion source */

      a as
      (

      /* android1 */ /* 1 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      anonymous_id,
      event as event_type,
      'Android' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      CAST(platform_id AS int64) as platform_id,
      episode,
      cast(is_chromecast as int64) as tv_cast,
      promotion
      from android.firstplay as a
      left join titles_id_mapping as b
      on a.video_id = b.id

      union all

      /* roku1 */ /* 2 */
      select
      timestamp,
      b.date as release_date,
      end_date,
      case
      when b.collection in ('Season 1','Season 2','Season 3') then concat(b.series,' ',b.collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when b.series is not null then 'series'
      else 'other'
      end as type,
      mysql_roku_firstplays_video_id as video_id,
      series,
      trim(b.title) as title,
      user_id,
      'anonymous_id' as anonymous_id,
      'firstplay' as event_type,
      'Roku' as source,
      UNIX_SECONDS(timestamp) as EPOCH_TIMESTAMP,
      CAST('1111' AS int64) as platform_id,
      b.episode,
      null as tv_cast,
      promotion
      from a32 as a
      left join titles_id_mapping as b
      on mysql_roku_firstplays_video_id = b.id

      union all

      /* ios1 */ /* 3 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      anonymous_id,
      event as event_type,
      'iOS' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      CAST(platform_id AS int64) as platform_id,
      episode,
      cast(is_chromecast as int64) + cast(is_airplay as int64) as tv_cast,
      promotion
      from ios.firstplay as a
      left join titles_id_mapping as b
      on a.video_id = safe_cast(b.id as string)

      union all

      /* roku2 */ /* 4 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      null as anonymous_id,
      event as event_type,
      'Roku' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      CAST(platform_id AS int64) as platform_id,
      episode,
      cast(is_chromecast as int64) + cast(is_airplay as int64) as tv_cast,
      promotion
      from roku.firstplay as a
      left join titles_id_mapping as b
      on a.video_id = b.id

      union all

      /* web1 */ /* 5 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      anonymous_id,
      'firstplay' as event_type,
      'Web' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      CAST(platform_id AS int64) as platform_id,
      episode,
      null as tv_cast,
      promotion
      from javascript.loadedmetadata as a
      left join titles_id_mapping as b
      on safe_cast(a.video_id as string) = safe_cast(b.id as string)

      union all

      /* web2 */ /* 6 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      anonymous_id,
      'firstplay' as event_type,
      'Web' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      CAST(platform_id AS int64) as platform_id,
      episode,
      null as tv_cast,
      promotion
      from javascript.video_content_playing as a
      left join titles_id_mapping as b
      on safe_cast(a.video_id as string) = safe_cast(b.id as string)

      union all

      /* ios2 */ /* 7 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      anonymous_id,
      'firstplay' as event_type,
      'iOS' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      CAST(platform_id AS int64) as platform_id,
      episode,
      null as tv_cast,
      promotion
      from ios.video_content_playing as a
      left join titles_id_mapping as b
      on safe_cast(a.video_id as string) = safe_cast(b.id as string)

      union all

      /* android2 */ /* 8 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      anonymous_id,
      'firstplay' as event_type,
      'Android' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      CAST(platform_id AS int64) as platform_id,
      episode,
      null as tv_cast,
      promotion
      from android.video_content_playing as a
      left join titles_id_mapping as b
      on safe_cast(a.video_id as string) = safe_cast(b.id as string)

      union all

      /* amazon */ /* 9 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      anonymous_id,
      'firstplay' as event_type,
      'FireTV' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      CAST(platform_id AS int64) as platform_id,
      episode,
      null as tv_cast,
      promotion
      from amazon_fire_tv.video_content_playing as a
      left join titles_id_mapping as b
      on safe_cast(a.video_id as string) = safe_cast(b.id as string)

      union all

      /* roku 3 */ /* 10 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      a.id as anonymous_id,
      'firstplay' as event_type,
      'Roku' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      CAST(platform_id AS int64) as platform_id,
      episode,
      null as tv_cast,
      promotion
      from roku.video_content_playing as a
      left join titles_id_mapping as b
      on safe_cast(a.video_id as string) = safe_cast(b.id as string)

      union all

      /* tizen */ /* 11 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      null as anonymous_id,
      'firstplay' as event_type,
      'Tizen' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      null as platform_id,
      episode,
      null as tv_cast,
      promotion
      from php.get_tizen_views as a
      left join titles_id_mapping as b
      on safe_cast(a.video_id as string) = safe_cast(b.id as string)

      union all

      /* xbox */ /* 12 */
      select
      sent_at as timestamp,
      b.date as release_date,
      end_date,
      case
      when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection)
      else collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(a.video_id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      null as anonymous_id,
      'firstplay' as event_type,
      'Xbox' as source,
      UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
      null as platform_id,
      episode,
      null as tv_cast,
      promotion
      from php.get_xbox_views as a left join
      titles_id_mapping as b
      on safe_cast(a.video_id as string) = safe_cast(b.id as string)

      union all

      /* generic */ /* 13 */
      select
      timestamp,
      b.date as release_date,
      end_date,
      case
      when b.collection in ('Season 1','Season 2','Season 3') then concat(series,' ',b.collection)
      else b.collection
      end as collection,
      case
      when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
      when series is not null then 'series'
      else 'other'
      end as type,
      safe_cast(b.id as int64) as video_id,
      series,
      trim(b.title) as title,
      user_id,
      '' as anonymous_id,
      'firstplay' as event_type,
      'Web' as source,
      UNIX_SECONDS(timestamp) as EPOCH_TIMESTAMP,
      CAST('33064' AS int64) as platform_id,
      episode,
      null as tv_cast,
      promotion
      from a2 as a
      left join titles_id_mapping as b
      on trim(upper(b.title)) = trim(upper(a.title))

      ),

      allfirstplay as (

      /* join master dataset with winback and first time customers table to finish query */
      select
      a.user_id,
      a.anonymous_id,
      a.event_type,
      timestamp_sub(a.timestamp,interval 4 hour) as timestamp,
      a.EPOCH_TIMESTAMP,
      a.platform_id,
      a.release_date,
      a.end_date,
      date_diff(date(timestamp_sub(a.timestamp,interval 4 hour)),a.release_date,day) as days_since_release,
      a.collection,
      a.type,
      a.video_id,
      series,
      a.title,
      a.source,
      a.episode,
      email,
      tv_cast,
      c.promotion,
      case
      when cc.user_id is null then 'first-time customers'
      else 'reacquisitions'
      end as winback,
      case
      when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER)
      and DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
      when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER)
      and DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
      when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER)
      and DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
      else "NA"
      end as Quarter
      from a left join cc
      on a.user_id = cc.user_id
      left join svod_titles.promos as c
      on a.video_id = c.video_id
      ),

      pe_last as
      (
      select user_id, topic, email, moptin, subscription_status, platform,
      row_number() over (partition by user_id order by timestamp desc) as event_num,
      date(timestamp) as date_stamp, subscription_frequency
      from http_api.purchase_event
      where regexp_contains(user_id, r'^[0-9]*$')
      and user_id <> '0'
      order by user_id, date(timestamp)
      ),

      user_optin as
      (
      select user_id, email, moptin, subscription_status
      from pe_last
      where event_num = 1
      ),

      play_data_global as
      (
      select * from allfirstplay
      where user_id <> '0'
      and regexp_contains(user_id, r'^[0-9]*$')
      and user_id is not null
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

      hudson_p0 as
      (
      select *,
      round(min_count/44,2) as cr
      from plays_less_granular
      where title = 'Season Finale: 219 - In a Family Way'
      and min_count >= 35
      order by user_id
      ),

      hudson_p1 as
      (
      select user_id, title,
      max(cr) as max_cr,
      max(min_count) as max_mins
      from hudson_p0
      group by user_id, title
      ),

      hudson_p2 as
      (
      select a.*, b.moptin, b.email
      from hudson_p1 as a
      left join user_optin as b
      on a.user_id = b.user_id
      )

      select * from hudson_p2 where moptin = true
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

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: max_cr {
    type: number
    sql: ${TABLE}.max_cr ;;
  }

  dimension: max_mins {
    type: number
    sql: ${TABLE}.max_mins ;;
  }

  dimension: moptin {
    type: yesno
    sql: ${TABLE}.moptin ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  set: detail {
    fields: [
      user_id,
      title,
      max_cr,
      max_mins,
      moptin,
      email
    ]
  }
}
