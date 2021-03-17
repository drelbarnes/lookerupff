view: bigquery_ribbon_plays {
  derived_table: {
    sql:WITH bigquery_allfirstplay AS (with aa as
      (select user_id,email,status_date as churn_date
      from http_api.purchase_event
      where topic in ('customer.product.cancelled','customer.product.disabled','customer.product.expired')),

      bb as
      (select user_id, email, max(status_date) as status_date
      from http_api.purchase_event
      where topic in ('customer.product.created','customer.product.renewed','customer.created','customer.product.free_trial_created')
      group by 1,2),

      cc as
      (select distinct bb.user_id, bb.email
      from aa inner join bb on aa.user_id=bb.user_id and status_date>churn_date),

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

      a32 as
      (select distinct mysql_roku_firstplays_firstplay_date_date as timestamp,
                      mysql_roku_firstplays_video_id,
                      user_id,
                      '' as anonymousId,
                      'firstplay' as event_type,
                      UNIX_SECONDS(mysql_roku_firstplays_firstplay_date_date) as EPOCH_TIMESTAMP,
                      CAST('1111' AS int64) as platform_id
      from looker.roku_firstplays),

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
               from a2 as a left join titles_id_mapping as b on trim(upper(b.title)) = trim(upper(a.title)))

      select a.user_id,
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
      from a left join cc on a.user_id=cc.user_id left join svod_titles.promos as c on a.video_id=c.video_id
      /* where a.user_id<>'0' */ ),

      bigquery_ribbon AS (SELECT distinct (mysql_upff_category_items_updated_at ) AS ingested_at,
             mysql_upff_category_items_name AS collection
      FROM looker.get_category_collections
       ),

      ribbon as
      (SELECT
        bigquery_ribbon.collection,
        CAST(bigquery_ribbon.ingested_at  AS DATE) AS ingest_at_date,
        COUNT(DISTINCT case when bigquery_allfirstplay.user_id is null or bigquery_allfirstplay.user_id='0' or length(bigquery_allfirstplay.user_id)=0 then concat(safe_cast(bigquery_allfirstplay.video_id as string),bigquery_allfirstplay.anonymous_id,cast((CAST(bigquery_allfirstplay.timestamp  AS DATE)) as string)) else concat(safe_cast(bigquery_allfirstplay.video_id as string),bigquery_allfirstplay.user_id,cast((CAST(bigquery_allfirstplay.timestamp  AS DATE)) as string)) end ) AS ribbon_plays
      FROM bigquery_allfirstplay
      inner JOIN bigquery_ribbon ON bigquery_ribbon.collection=bigquery_allfirstplay.collection

      WHERE (((TIMESTAMP_TRUNC(CAST(bigquery_allfirstplay.timestamp  AS TIMESTAMP), DAY)) >= (TIMESTAMP_TRUNC(CAST(bigquery_ribbon.ingested_at  AS TIMESTAMP), DAY))) AND ((TIMESTAMP_TRUNC(CAST(bigquery_allfirstplay.timestamp  AS TIMESTAMP), DAY)) <= TIMESTAMP_ADD((TIMESTAMP_TRUNC(CAST(bigquery_ribbon.ingested_at  AS TIMESTAMP), DAY)), INTERVAL 7 DAY)))
      GROUP BY 1,2),

      pre_ribbon1 as
      (SELECT
        bigquery_ribbon.collection,
        CAST(bigquery_ribbon.ingested_at  AS DATE) AS ingest_at_date,
        COUNT(DISTINCT case when bigquery_allfirstplay.user_id is null or bigquery_allfirstplay.user_id='0' or length(bigquery_allfirstplay.user_id)=0 then concat(safe_cast(bigquery_allfirstplay.video_id as string),bigquery_allfirstplay.anonymous_id,cast((CAST(bigquery_allfirstplay.timestamp  AS DATE)) as string)) else concat(safe_cast(bigquery_allfirstplay.video_id as string),bigquery_allfirstplay.user_id,cast((CAST(bigquery_allfirstplay.timestamp  AS DATE)) as string)) end ) AS pre_ribbon_plays
      FROM bigquery_allfirstplay
      inner JOIN bigquery_ribbon ON bigquery_ribbon.collection=bigquery_allfirstplay.collection
      WHERE (((TIMESTAMP_TRUNC(CAST(bigquery_allfirstplay.timestamp  AS TIMESTAMP), DAY)) < (TIMESTAMP_TRUNC(CAST(bigquery_ribbon.ingested_at  AS TIMESTAMP), DAY))) AND ((TIMESTAMP_TRUNC(CAST(bigquery_allfirstplay.timestamp  AS TIMESTAMP), DAY)) > TIMESTAMP_ADD((TIMESTAMP_TRUNC(CAST(bigquery_ribbon.ingested_at  AS TIMESTAMP), DAY)), INTERVAL -7 DAY)))
      GROUP BY 1,2),

pre_ribbon2 as
(select collection,
        min(ingest_at_date) over (partition by collection order by ingest_at_date rows between 2 preceding and current row) as ingest_at_date
from pre_ribbon1),

pre_ribbon3 as
(select distinct * from pre_ribbon2),

pre_ribbon as
(select a.* from pre_ribbon1 as a inner join pre_ribbon3 as b on a.collection=b.collection and a.ingest_at_date=b.ingest_at_date)

      select timestamp(b.ingest_at_date) as ingest_at_date,
             b.collection,
             mysql_upff_category_items_cat_name as category_name,
             mysql_upff_category_items_cat_order as cat_order,
             pre_ribbon_plays,
             ribbon_plays
      from ribbon as a inner join pre_ribbon as b on a.collection=b.collection and a.ingest_at_date=b.ingest_at_date inner join looker.get_category_collections as c on a.collection=mysql_upff_category_items_name and a.ingest_at_date=date(mysql_upff_category_items_updated_at);;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: category_name {
    type: string
    sql: ${TABLE}.category_name ;;
  }

  dimension_group: ingest_at {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.ingest_at_date ;;
  }

  dimension: cat_order {
    type: number
    sql: ${TABLE}.cat_order ;;
  }

  dimension: item_order {
    type: number
    sql: ${TABLE}.item_order ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: pre_ribbon_plays {
    type: number
    sql: ${TABLE}.pre_ribbon_plays ;;
  }

  measure: pre_ribbon_plays_ {
    type: sum
    sql: ${pre_ribbon_plays} ;;
  }

  dimension: ribbon_plays {
    type: number
    sql: ${TABLE}.ribbon_plays ;;
  }

  measure: ribbon_plays_ {
    type: sum
    sql: ${ribbon_plays} ;;
  }

  set: detail {
    fields: [
      category_name,
      ingest_at_date,
      cat_order,
      item_order,
      collection,
      pre_ribbon_plays,
      ribbon_plays
    ]
  }
}
