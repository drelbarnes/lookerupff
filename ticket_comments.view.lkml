view: ticket_comments {
  derived_table: {
    sql: with

      ticket_comments as
      (
      select
        ticket_id,
        ltrim(string_agg(concat(' ', body))) as body
      from zendesk.ticket_comments_view
      group by ticket_id
      ),

      merged_ticket_data as
      (
      select
      distinct
      a.id,
      date(a.created_at) as crdt,
      a.description,
      a.subject,
      a.tags,
      a.type,
      a.category,
      a.feedback,
      case
      when a.feedback like '%positive%' then 'positive'
      when a.feedback like '%negative%' then 'negative'
      else 'missing'
      end as flag,
      b.body
      from zendesk.tickets_view as a
      inner join ticket_comments as b
      on a.id = b.ticket_id
      )

      select * from merged_ticket_data
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: crdt {
    type: date
    datatype: date
    sql: ${TABLE}.crdt ;;
  }

  dimension: description {
    type: string
    sql: ${TABLE}.description ;;
  }

  dimension: subject {
    type: string
    sql: ${TABLE}.subject ;;
  }

  dimension: tags {
    type: string
    sql: ${TABLE}.tags ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: feedback {
    type: string
    sql: ${TABLE}.feedback ;;
  }

  dimension: flag {
    type: string
    sql: ${TABLE}.flag ;;
  }

  dimension: body {
    type: string
    sql: ${TABLE}.body ;;
  }

  set: detail {
    fields: [
      id,
      crdt,
      description,
      subject,
      tags,
      type,
      category,
      feedback,
      flag,
      body
    ]
  }
}
