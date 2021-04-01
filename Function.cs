using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;
using Amazon.DynamoDBv2;
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DataModel;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Assignment7
{
    [DynamoDBTable("ItemsForSale")]
    public class Item
    {
        [DynamoDBHashKey]
        public string itemId { get; set; }

        public double rating { get; set; }
        public string type { get; set; }
    }
    public class Function
    {
        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        public async Task<List<Item>> FunctionHandler(DynamoDBEvent input, ILambdaContext context)
        {
            Table table = Table.LoadTable(client, "RatingsByType");
            List<Item> items = new List<Item>();
            List<DynamoDBEvent.DynamodbStreamRecord> records = (List<DynamoDBEvent.DynamodbStreamRecord>)input.Records;
            if (records.Count > 0)
            {
                DynamoDBEvent.DynamodbStreamRecord record = records[0];
                if (record.EventName.Equals("INSERT"))
                {
                    Document myDoc = Document.FromAttributeMap(record.Dynamodb.NewImage);
                    Item myItem = JsonConvert.DeserializeObject<Item>(myDoc.ToJson());

                    // collection scan to get average
                    var dbContext = new DynamoDBContext(client);
                    List<ScanCondition> conditions = new List<ScanCondition>();
                    conditions.Add(new ScanCondition("type", ScanOperator.Equal, myItem.type));
                    var response = await dbContext.ScanAsync<Item>(conditions).GetRemainingAsync();

                    double count = 0;
                    double average = 0;
                    double total = 0;
                    foreach (Item singleItem in response)
                    {
                        count++;
                        total += singleItem.rating;
                    }
                    average = total / count;

                    var request = new UpdateItemRequest
                    {
                        TableName = "RatingsByType",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            { "type", new AttributeValue { S = myItem.type } }
                        },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            {
                                "count",
                                new AttributeValueUpdate { Action = "ADD", Value = new AttributeValue { N = "1" } }
                            },
                            {
                                "averageRating",
                                new AttributeValueUpdate { Action = "PUT", Value = new AttributeValue { N = average.ToString() } }
                            },
                        },

                    };
                    await client.UpdateItemAsync(request);
                }
            }
            return items;
        }
    }
}
