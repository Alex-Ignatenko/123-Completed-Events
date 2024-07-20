using YamlDotNet.Serialization;
public class ConfigReader
{
     public int ReporterIdStartIndex { get; private set; }
    public int ReporterIdJumpValue { get; private set; }
    public int MetricIdMinValue { get; private set; }
    public int MetricIdMaxValue { get; private set; }
    public int MetricValueMinValue { get; private set; }
    public int MetricValueMaxValue { get; private set; }
    public string? Message { get; private set; }
    public int MessageInterval { get; private set; }
    public string? KafkaServerInfo { get; private set; }
    public string? KafkaTopic { get; private set; }

    public ConfigReader(string  configFilePath)
    {
        SetValuesFromFile(configFilePath);    
    }

    //Get path to yml file.
    //Deserializes it into a Dictionary<string, string> and populates ConfigReader class attributes.
    private void SetValuesFromFile(string configFilePath)
    { 
        try
        {
            using (var reader = new StringReader(File.ReadAllText(configFilePath)))
            {  
                var deserializer = new DeserializerBuilder().Build();
                var parsedYML = deserializer.Deserialize<Dictionary<string, string>>(reader);

                ReporterIdStartIndex = int.Parse(parsedYML["Reporter ID Start Index"]);
                ReporterIdJumpValue = int.Parse(parsedYML["Reporter ID Jump Value"]);
                MetricIdMinValue = int.Parse(parsedYML["Metric ID Min Value"]);
                MetricIdMaxValue = int.Parse(parsedYML["Metric ID Max Value"]);
                MetricValueMinValue = int.Parse(parsedYML["Metric Value Min Value"]);
                MetricValueMaxValue = int.Parse(parsedYML["Metric Value Max Value"]);
                Message = parsedYML["Message"];
                MessageInterval = int.Parse(parsedYML["Message Interval"]);
                KafkaServerInfo = parsedYML["Kafka Server Info"];
                KafkaTopic = parsedYML["Kafka Topic"];
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

    }
}