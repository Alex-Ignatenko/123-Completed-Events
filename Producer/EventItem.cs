public class EventItem
{
    public int ReporterId { get; set; }
    public DateTime Timestamp { get; set; }
    public int MetricId { get; set; }
    public int MetricValue { get; set; }
    public string? Message { get; set; }

    public EventItem(int reporterId, DateTime timestamp, int metricId, int metricValue, string message)
    {
        ReporterId = reporterId;
        Timestamp = timestamp;
        MetricId = metricId;
        MetricValue = metricValue;
        Message = message;
    }
}