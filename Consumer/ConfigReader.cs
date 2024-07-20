using YamlDotNet.Serialization;
public class ConfigReader 
{
    public string? TimeFormat { get; set; }
    public string? KafkaServerInfo { get; private set; }
    public string? KafkaTopic { get; private set; }
    public string? KafkaGroupId { get; private set; }
    public string? MongoConnectionString { get; private set; }
    public string? MongoDbName { get; private set; }
    public string? MongoCollectionName { get; private set; }

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

                TimeFormat = parsedYML["Time Format"];
                KafkaServerInfo = parsedYML["Kafka Server Info"];
                KafkaTopic = parsedYML["Kafka Topic"];
                KafkaGroupId = parsedYML["Kafka Group Id"];
                MongoConnectionString = parsedYML["Mongo Connection String"];
                MongoDbName = parsedYML["Mongo DB Name"];
                MongoCollectionName = parsedYML["Mongo Collection Name"];
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

    }
}