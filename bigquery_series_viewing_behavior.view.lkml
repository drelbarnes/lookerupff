view: bigquery_flight29 {
  derived_table: {
    sql:
    /*Begin by building out queries to segment new customer views and winback views*/

    with aa as
    (select user_id,email,status_date as churn_date
    from http_api.purchase_event
    where topic in ('customer.product.cancelled','customer.product.disabled','customer.product.expired')),

    bb as
    (select user_id, email, max(status_date) as status_date
    from http_api.purchase_event
    where topic in ('customer.product.created','customer.product.renewed','customer.created','customer.product.free_trial_created')
    group by 1,2),

    /*Create table with customers who have a status data after churning*/
    cc as
    (select distinct bb.user_id, bb.email
    from aa inner join bb on aa.user_id=bb.user_id and status_date>churn_date),

    /*For older dates, we leverage firstplay tables.*/
    a1 as
    (select sent_at as timestamp,
            user_id,
            (split(title," - ")) as title
    from javascript.firstplay),

    a2 as
    (select timestamp,
            user_id,
            title[safe_ordinal(1)] as title,
            concat(title[safe_ordinal(2)]," - ",title[safe_ordinal(3)]) as collection
     from a1 order by 1),

    /*Use php.get_titles table to create title id mapping table that maps video id to title of any given asset*/
    a30 as
    (select video_id,
           max(loaded_at) as loaded_at
    from php.get_titles
    group by 1),

    titles_id_mapping as
    (select distinct
           metadata_series_name  as series,
           case when metadata_season_name in ('Season 1','Season 2','Season 3') then concat(metadata_series_name,'-',metadata_season_name)
                when metadata_season_name is null then metadata_movie_name
                else metadata_season_name end as collection,
           season_number as season,
           a.title,
           a.video_id as id,
           episode_number as episode,
           date(time_available) as date,
           date(time_unavailable) as end_date,
           round(duration_seconds/60) as duration,
           promotion
    from php.get_titles as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id inner join a30 on a30.video_id=a.video_id and a30.loaded_at=a.loaded_at),
    /*call legacy roku firstplay table for old dates*/
    a32 as
    (select distinct mysql_roku_firstplays_firstplay_date_date as timestamp,
                    mysql_roku_firstplays_video_id,
                    user_id,
                    '' as anonymousId,
                    'firstplay' as event_type,
                    UNIX_SECONDS(mysql_roku_firstplays_firstplay_date_date) as EPOCH_TIMESTAMP,
                    CAST('1111' AS int64) as platform_id
    from looker.roku_firstplays),
    /*build master dataset for engagement using firstplay tables for older dates and the current video_content_playing tables for
    current engagement ingestion source*/
    a as
            (select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from android.firstplay as a left join titles_id_mapping as b on a.video_id = b.id

             union all

            select timestamp,
           b.date as release_date,
           end_date,
           case when b.collection in ('Season 1','Season 2','Season 3') then concat(b.series,' ',b.collection) else collection end as collection,
           case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when b.series is not null then 'series' else 'other' end as type,
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
    from a32 as a left join titles_id_mapping as b on mysql_roku_firstplays_video_id=b.id


             union all

             select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
                    cast(is_chromecast as int64)+cast(is_airplay as int64) as tv_cast,
                    promotion
             from ios.firstplay as a left join titles_id_mapping as b on a.video_id = safe_cast(b.id as string)
             union all
             select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
                    cast(is_chromecast as int64)+cast(is_airplay as int64) as tv_cast,
                    promotion
             from roku.firstplay as a left join titles_id_mapping as b on a.video_id = b.id
             union all
             select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from javascript.loadedmetadata as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)
            union all
            select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from javascript.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

            union all

    select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from ios.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

            union all

            select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from android.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

            union all

            select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from amazon_fire_tv.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

            union all

            select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from roku.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

             union all

            select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from php.get_tizen_views as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

             union all

             select sent_at as timestamp,
                    b.date as release_date,
                    end_date,
                    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from php.get_xbox_views as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

            union all

            select timestamp,
                    b.date as release_date,
                    end_date,
                    case when b.collection in ('Season 1','Season 2','Season 3') then concat(series,' ',b.collection) else b.collection end as collection,
                    case when (series is null and upper(b.title) like upper(b.collection))  then 'movie'
                         when series is not null then 'series' else 'other' end as type,
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
             from a2 as a left join titles_id_mapping as b on trim(upper(b.title)) = trim(upper(a.title))),
    /*join master dataset with winback and first time customers table to finish query*/

    master as
    (select a.user_id,
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
           case when cc.user_id is null then 'first-time customers' else 'reacquisitions' end as winback,
           case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
                DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
                when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
                DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
                when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
                DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
                else "NA"
                end as Quarter
    from a left join cc on a.user_id=cc.user_id left join svod_titles.promos as c on a.video_id=c.video_id),

  /* creates viewership flags for each episode per user_id */

episodes as
(select a.user_id,
  a.collection,
  a.series,
  a.title,
  a.source,
  a.episode,
from a left join cc on a.user_id=cc.user_id left join svod_titles.promos as c on a.video_id=c.video_id),

flags as
(select
  user_id, collection, episode,
  case when episode=1 then 1 else 0 end as ep1,
  case when episode=2 then 1 else 0 end as ep2,
  case when episode=3 then 1 else 0 end as ep3,
  case when episode=4 then 1 else 0 end as ep4,
  case when episode=5 then 1 else 0 end as ep5,
  case when episode=6 then 1 else 0 end as ep6,
  case when episode=7 then 1 else 0 end as ep7,
  case when episode=8 then 1 else 0 end as ep8,
  case when episode=9 then 1 else 0 end as ep9,
  case when episode=10 then 1 else 0 end as ep10,
  case when episode=11 then 1 else 0 end as ep11,
  case when episode=12 then 1 else 0 end as ep12,
  case when episode=13 then 1 else 0 end as ep13
from episodes
group by 1,2,3,4
order by 1,2,3,4),

max as
(select
  user_id, collection,
  max(ep1) as ep1,
  max(ep2) as ep2,
  max(ep3) as ep3,
  max(ep4) as ep4,
  max(ep5) as ep5,
  max(ep6) as ep6,
  max(ep7) as ep7,
  max(ep8) as ep8,
  max(ep9) as ep9,
  max(ep10) as ep10,
  max(ep11) as ep11,
  max(ep12) as ep12,
  max(ep13) as ep13,
  max(ep14) as ep14,
  max(ep15) as ep15,
  max(ep16) as ep16,
  max(ep17) as ep17,
  max(ep18) as ep18,
  max(ep19) as ep19,
  max(ep20) as ep20
from flags
group by 1,2),

sum as
(select
  user_id, collection,
  ep1, ep2, ep3, ep4, ep5, ep6, ep7, ep8, ep9, ep10,
  ep11, ep12, ep13, ep14, ep15, ep16, ep17, ep18, ep19, ep20,
  ep1+ep2+ep3+ep4+ep5+ep6+ep7+ep8+ep9+ep10+ep11+ep12+ep13+ep14+ep15+ep16+ep17+ep18+ep19+ep20 as total_episodes
from max
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22),

habits as
(select user_id, collection, total_episodes,
  case
    when total_episodes=1 then 'A: First episode only'
    when total_episodes>1 and total_episodes<5 then 'B: 2-4 episodes'
    when total_episodes>4 and total_episodes<13 then 'C: 5-12 episodes'
    when total_episodes=13 then 'D: 13+ episodes'
  end as viewing_habit
from sum
group by 1,2,3
order by 1,2,3)

select
  viewing_habit,
  collection,
  count(user_id) as user_count
from habits
group by 1,2
  ;;
}

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: viewing_habit {
    type: string
    sql: ${TABLE}.viewing_habit ;;
  }

  measure: user_count {
    type: sum
    sql: ${TABLE}.user_count ;;
  }

}
