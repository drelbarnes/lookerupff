view: amazon_personalize_recommendations {
  derived_table: {
    sql: with a as
      (SELECT
        distinct http_api_purchase_event.email  AS "email",
        redshift_get_titles.title  AS "rOnetitle",
        redshift_get_titles.thumbnail  AS "rOnethumbnail",
        redshift_get_titles.short_description  AS "rOneshort_description",
        redshift_get_titles.url  AS "rOneurl",
        redshift_python_users.id  AS "id"
      FROM python.users  AS redshift_python_users
      LEFT JOIN http_api.purchase_event  AS http_api_purchase_event ON http_api_purchase_event.user_id = redshift_python_users.id
      LEFT JOIN php.get_titles  AS redshift_get_titles ON (CAST(redshift_python_users.recommended_title_one as INT)) = redshift_get_titles.video_id

      WHERE ((((redshift_python_users.received_at ) >= ((DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ))) AND (redshift_python_users.received_at ) < ((DATEADD(day,14, DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ) )))))) AND ((redshift_get_titles.url IS NOT NULL))
      ORDER BY 1 ),

      b as
      (SELECT
        distinct http_api_purchase_event.email  AS "email",
        redshift_get_titles.title  AS "rTwotitle",
        redshift_get_titles.thumbnail  AS "rTwothumbnail",
        redshift_get_titles.short_description  AS "rTwoshort_description",
        redshift_get_titles.url  AS "rTwourl",
        redshift_python_users.id  AS "id"
      FROM python.users  AS redshift_python_users
      LEFT JOIN http_api.purchase_event  AS http_api_purchase_event ON http_api_purchase_event.user_id = redshift_python_users.id
      LEFT JOIN php.get_titles  AS redshift_get_titles ON (CAST(redshift_python_users.recommended_title_two as INT)) = redshift_get_titles.video_id

      WHERE ((((redshift_python_users.received_at ) >= ((DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ))) AND (redshift_python_users.received_at ) < ((DATEADD(day,14, DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ) )))))) AND ((redshift_get_titles.url IS NOT NULL))
      ORDER BY 1 ),

      c as
      (SELECT
        distinct http_api_purchase_event.email  AS "email",
        redshift_get_titles.title  AS "rThreetitle",
        redshift_get_titles.thumbnail  AS "rThreethumbnail",
        redshift_get_titles.short_description  AS "rThreeSum",
        redshift_get_titles.url  AS "rThreeurl",
        redshift_python_users.id  AS "id"
      FROM python.users  AS redshift_python_users
      LEFT JOIN http_api.purchase_event  AS http_api_purchase_event ON http_api_purchase_event.user_id = redshift_python_users.id
      LEFT JOIN php.get_titles  AS redshift_get_titles ON (CAST(redshift_python_users.recommended_title_three as INT)) = redshift_get_titles.video_id

      WHERE ((((redshift_python_users.received_at ) >= ((DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ))) AND (redshift_python_users.received_at ) < ((DATEADD(day,14, DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ) )))))) AND ((redshift_get_titles.url IS NOT NULL))
      ORDER BY 1 ),

      d as
      (SELECT
        distinct http_api_purchase_event.email  AS "email",
        redshift_get_titles.title  AS "rFourtitle",
        redshift_get_titles.thumbnail  AS "rFourImg",
        redshift_get_titles.short_description  AS "rFourSum",
        redshift_get_titles.url  AS "rFourUrl",
        redshift_python_users.id  AS "id"
      FROM python.users  AS redshift_python_users
      LEFT JOIN http_api.purchase_event  AS http_api_purchase_event ON http_api_purchase_event.user_id = redshift_python_users.id
      LEFT JOIN php.get_titles  AS redshift_get_titles ON (CAST(redshift_python_users.recommended_title_four as INT)) = redshift_get_titles.video_id

      WHERE ((((redshift_python_users.received_at ) >= ((DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ))) AND (redshift_python_users.received_at ) < ((DATEADD(day,14, DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ) )))))) AND ((redshift_get_titles.url IS NOT NULL))
      ORDER BY 1 ),

      e as
      (SELECT
        distinct http_api_purchase_event.email  AS "email",
        redshift_get_titles.title  AS "rFiveTitle",
        redshift_get_titles.thumbnail  AS "rFiveImg",
        redshift_get_titles.short_description  AS "rFiveSum",
        redshift_get_titles.url  AS "rFiveUrl",
        redshift_python_users.id  AS "id"
      FROM python.users  AS redshift_python_users
      LEFT JOIN http_api.purchase_event  AS http_api_purchase_event ON http_api_purchase_event.user_id = redshift_python_users.id
      LEFT JOIN php.get_titles  AS redshift_get_titles ON (CAST(redshift_python_users.recommended_title_five as INT)) = redshift_get_titles.video_id

      WHERE ((((redshift_python_users.received_at ) >= ((DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ))) AND (redshift_python_users.received_at ) < ((DATEADD(day,14, DATEADD(day,-13, DATE_TRUNC('day',GETDATE()) ) )))))) AND ((redshift_get_titles.url IS NOT NULL))
      ORDER BY 1 )

      select
      a.email,
      a.id,
      rOneTitle, rOnethumbnail, rOneshort_description, rOneurl,
      rTwotitle, rTwothumbnail, rTwoshort_description, rTwourl,
      rThreetitle, rThreethumbnail, rThreeSum, rThreeurl,
      rFourtitle, rFourImg, rFourSum, rFourUrl,
      rFiveTitle, rFiveImg, rFiveSum, rFiveUrl
      from a, b, c, d, e WHERE a.id = b.id AND b.id = c.id AND c.id = d.id AND d.id = e.id
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: ronetitle {
    type: string
    sql: ${TABLE}.ronetitle ;;
  }

  dimension: ronethumbnail {
    type: string
    sql: ${TABLE}.ronethumbnail ;;
  }

  dimension: roneshort_description {
    type: string
    sql: ${TABLE}.roneshort_description ;;
  }

  dimension: roneurl {
    type: string
    sql: ${TABLE}.roneurl ;;
  }

  dimension: rtwotitle {
    type: string
    sql: ${TABLE}.rtwotitle ;;
  }

  dimension: rtwothumbnail {
    type: string
    sql: ${TABLE}.rtwothumbnail ;;
  }

  dimension: rtwoshort_description {
    type: string
    sql: ${TABLE}.rtwoshort_description ;;
  }

  dimension: rtwourl {
    type: string
    sql: ${TABLE}.rtwourl ;;
  }

  dimension: rthreetitle {
    type: string
    sql: ${TABLE}.rthreetitle ;;
  }

  dimension: rthreethumbnail {
    type: string
    sql: ${TABLE}.rthreethumbnail ;;
  }

  dimension: rthreesum {
    type: string
    sql: ${TABLE}.rthreesum ;;
  }

  dimension: rthreeurl {
    type: string
    sql: ${TABLE}.rthreeurl ;;
  }

  dimension: rfourtitle {
    type: string
    sql: ${TABLE}.rfourtitle ;;
  }

  dimension: rfourimg {
    type: string
    sql: ${TABLE}.rfourimg ;;
  }

  dimension: rfoursum {
    type: string
    sql: ${TABLE}.rfoursum ;;
  }

  dimension: rfoururl {
    type: string
    sql: ${TABLE}.rfoururl ;;
  }

  dimension: rfivetitle {
    type: string
    sql: ${TABLE}.rfivetitle ;;
  }

  dimension: rfiveimg {
    type: string
    sql: ${TABLE}.rfiveimg ;;
  }

  dimension: rfivesum {
    type: string
    sql: ${TABLE}.rfivesum ;;
  }

  dimension: rfiveurl {
    type: string
    sql: ${TABLE}.rfiveurl ;;
  }

  set: detail {
    fields: [
      email,
      id,
      ronetitle,
      ronethumbnail,
      roneshort_description,
      roneurl,
      rtwotitle,
      rtwothumbnail,
      rtwoshort_description,
      rtwourl,
      rthreetitle,
      rthreethumbnail,
      rthreesum,
      rthreeurl,
      rfourtitle,
      rfourimg,
      rfoursum,
      rfoururl,
      rfivetitle,
      rfiveimg,
      rfivesum,
      rfiveurl
    ]
  }
}
