view: sales {
  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  measure: total_spent {
    type: sum
    sql: ${TABLE}.amount ;;
  }
}
