

case class DivvyReport(
           year: String,
           month: String,
           trips: Long,
           trip_duration: Long,
           subscriber: Long,
           non_subscriber: Long,
           avg_precip: Long,
           avg_snow: Long,
           avg_temp: Long,
           total_bus: Long,
           total_rail: Long)