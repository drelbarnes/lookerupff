view: redshift_segment_anonymous_known_users {
  derived_table: {
    sql: with a as
        (
          select distinct(anonymous_id) from ios.tracks
        ),

        b as
        (
          select distinct(anonymous_id) from android.tracks
        ),

        c as
        (
          select distinct(anonymous_id) from looker.tracks
        ),

        d as
        (
          select distinct(anonymous_id) from javascript.tracks
        ),

        e as (
          select * from a
            UNION
            select * from b
            UNION
            select * from c
            UNION
            select * from d
            )

        select count(*) from e
       ;;
  }

  dimension: count {
    type: number
    sql: ${TABLE}.count ;;
  }

  set: detail {
    fields: [count]
  }
}
