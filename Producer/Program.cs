using Newtonsoft.Json;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        ConfigReader configReader = InitConfigReader();

        ProducerConfig kafkaProducerConfig = new ProducerConfig{BootstrapServers = configReader.KafkaServerInfo};

        EventGenerator eventGenerator = new EventGenerator(configReader);
             
        try
        {
            using (var producer = new ProducerBuilder<Null, string>(kafkaProducerConfig).Build())
            {
                //Genertae a random event instance, convert it to JSON and send it to selected kafka topic
                while (true)
                {
                    EventItem eventItem = eventGenerator.CreateRandomEvent();
                    string eventItemJson = JsonConvert.SerializeObject(eventItem);

                    await producer.ProduceAsync(configReader.KafkaTopic, new Message<Null, string> {Value=eventItemJson});
                    Console.WriteLine($"Sent to kafka: {eventItemJson}");
             
                    Thread.Sleep(configReader.MessageInterval);
                }
            } 
        }
        catch (Exception e)
        {    
            Console.WriteLine(e.Message);
        }
    }

    private static ConfigReader InitConfigReader()
    {
        string currentDirectory = Directory.GetCurrentDirectory();   
        string configFilePath = Path.Combine(currentDirectory,"..","config.yml");
        ConfigReader configReader = new ConfigReader(configFilePath);
        return configReader;
    }
}




