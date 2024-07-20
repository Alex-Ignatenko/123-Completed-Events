using NRedisStack;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Globalization;

class Program
{
    static async Task Main(string[] args)
    {
        ConfigReader configReader = InitConfigReader();

        try
        {
            //MongoDB connection initialization
            MongoClient mongoClient = new MongoClient(configReader.MongoConnectionString);
            var eventsDb = mongoClient.GetDatabase(configReader.MongoDbName);
            var eventsCollection = eventsDb.GetCollection<BsonDocument>(configReader.MongoCollectionName);

            //Redis connection initialization 
            ConnectionMultiplexer redisConnection = ConnectionMultiplexer.Connect(configReader.REDISConnectionString!);
            IDatabase redisDb = redisConnection.GetDatabase();
            
            Console.WriteLine("-----------------------------------------------------------ETL woke up...-----------------------------------------------------------");
            while (true)
            {   
                //Get the latest timestamp saved in REDIS
                string? timestampFromRedis = redisDb.StringGet("Last Timestamp");

                //Set sort and filter parameters
                SortDefinition<BsonDocument> sortParam = Builders<BsonDocument>.Sort.Ascending("Timestamp");
                FilterDefinition<BsonDocument> filterParam = GetMongoFilter("Timestamp",timestampFromRedis!);

                //Read from mongoDb
                List<BsonDocument> documents = await eventsCollection.Find(filterParam).Sort(sortParam).ToListAsync();

                //If read collection isnt empty load it to REDIS and update latest timestamp in REDIS
                if(documents.Count > 0)
                {
                    await LoadMongoToRedisAsync(configReader,documents, redisDb);
                    SetLastTimestamp(configReader,documents, redisDb);
                    Console.WriteLine($"Last saved timestamp in REDIS: {redisDb.StringGet("Last Timestamp")}");
                }

                Thread.Sleep(configReader.ETLWakeupTimer);
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

    private static string SetLastTimestamp(ConfigReader configReader,List<BsonDocument> documents, IDatabase redisDb)
    {
        string lastTimestamp ="";

        //Get last timestamp from the sorted collection and parse it into a timestamp string with selected time format
        if(documents.Count > 0)
        {
            BsonDocument lastDocument = documents[documents.Count -1];
            BsonDateTime stampFromDoc = BsonDateTime.Create(DateTime.Parse(lastDocument["Timestamp"].ToString()!));
            lastTimestamp = stampFromDoc.ToUniversalTime().ToString(configReader.TimeFormatData);
        }

        redisDb.StringSet("Last Timestamp", lastTimestamp);

        return lastTimestamp;
    }

    //Returns an empty filter or a greater than filter if the value isnt null
    private static FilterDefinition<BsonDocument> GetMongoFilter(string filterKey, string filterValue)
    {
        
        FilterDefinition<BsonDocument> filterParam = Builders<BsonDocument>.Filter.Empty;

        if (!string.IsNullOrEmpty(filterValue))
            filterParam = Builders<BsonDocument>.Filter.Gt(filterKey,filterValue); 

        return filterParam;
    }

    private static async Task LoadMongoToRedisAsync(ConfigReader configReader,List<BsonDocument> Documents, IDatabase redisDb)
    {   
        //Loop through the mongo collection and post each doc to redis as a key value pair
        //Redis key is a combination of a documents ReporterId and Timestamp fields
		//Timestamp part of the redis key is converted to selected time format 
        foreach (BsonDocument doc in Documents)
        {
            Console.WriteLine($"Recieved from mongo: {doc}");

            string convertedTimestamp = DateTime.Parse(doc["Timestamp"].ToString()!).ToString(configReader.TimeFormatKey);
            string redisKey = doc["ReporterId"].ToString() + ":"+ convertedTimestamp;
            await redisDb.StringSetAsync(redisKey, doc.ToJson());

            Console.WriteLine("Loaded from REDIS: " + await  redisDb.StringGetAsync(redisKey));
            Console.WriteLine("------------------------------------------------------------------------------------------------");
            Console.WriteLine("------------------------------------------------------------------------------------------------");
        }
    }
}



