public class EventGenerator 
{
    private ConfigReader ConfigReader { get; set; }
    private int ReporterIdCurrentIndex { get; set; }
    private int ReporterIdJumpValue  { get; set; } 
    private int MetricIdMinValue { get; set; }
    private int MetricIdMaxValue { get; set; }
    private int MetricValueMinValue { get; set; }
    private int MetricValueMaxValue { get; set; }
    string? Message { get; set; }

    public EventGenerator(ConfigReader configReader) 
    {   
        ConfigReader = configReader;
        SetValuesFromConfig(); 
    }

    private void SetValuesFromConfig() 
    {
        ReporterIdCurrentIndex = ConfigReader.ReporterIdStartIndex;
        ReporterIdJumpValue = ConfigReader.ReporterIdJumpValue;
        MetricIdMinValue = ConfigReader.MetricIdMinValue;
        MetricIdMaxValue = ConfigReader.MetricIdMaxValue;
        MetricValueMinValue = ConfigReader.MetricValueMinValue;
        MetricValueMaxValue = ConfigReader.MetricValueMaxValue;
        Message = ConfigReader.Message;
    }
    
    public EventItem CreateRandomEvent() 
    {
        int ReporterId = ReporterIdCurrentIndex;
        ReporterIdCurrentIndex += ReporterIdJumpValue;
           
        DateTime Timestamp = DateTime.Now;
        int metricId = new Random().Next(MetricIdMinValue, MetricIdMaxValue);
        int metricValue = new Random().Next(MetricValueMinValue, MetricValueMaxValue);

        EventItem eventItem = new(ReporterId,Timestamp,metricId,metricValue,Message!);
        return eventItem; 
    }
}