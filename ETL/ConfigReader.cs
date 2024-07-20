using YamlDotNet.Serialization;
public class ConfigReader 
{
    public string? TimeFormatData { get; set; }
    public string? TimeFormatKey { get; set; }
    public string? MongoConnectionString { get; private set; }
    public string? MongoDbName { get; private set; }
    public string? MongoCollectionName { get; private set; }
    public string? REDISConnectionString { get; private set; }
    public int ETLWakeupTimer { get; private set; }

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

                TimeFormatData = parsedYML["Time Format Data"];
                TimeFormatKey = parsedYML["Time Format Key"];
                MongoConnectionString = parsedYML["Mongo Connection String"];
                MongoDbName = parsedYML["Mongo DB Name"];
                MongoCollectionName = parsedYML["Mongo Collection Name"];
                REDISConnectionString = parsedYML["REDIS Connection String"];
                ETLWakeupTimer = int.Parse(parsedYML["ETL Wakeup Timer"]);
                MongoCollectionName = parsedYML["Mongo Collection Name"];
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

    }
}