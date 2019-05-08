using System;
using System.Threading.Tasks;

namespace producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var factory = new KafkaProducerFactory();

            using (var producer = factory.Create())
            {
                var result = await producer.ProduceAsync("maosk-test", new Confluent.Kafka.Message<string, string>()
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = "Hello world"
                });

                if (result.Status == Confluent.Kafka.PersistenceStatus.Persisted)
                    Console.WriteLine($"Message successfully persisted to topic {result.Topic}");
                else
                    Console.WriteLine($"Message perhaps not persisted: {result.Status}");
            }

            Console.WriteLine("Enter to quit...");
            Console.ReadLine();
        }
    }
}
