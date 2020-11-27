using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProducer
{
    class Program
    {
        public static async Task Main()
        {
            var config = new ProducerConfig { BootstrapServers = "127.0.0.1:9092" };

            using var p = new ProducerBuilder<Null, string>(config).Build();

            {
                try
                {
                    var count = 0;
                    while (true)
                    {
                        var dr = await p.ProduceAsync("topico-teste",
                            new Message<Null, string> { Value = $"teste: {count++}" });

                        Console.WriteLine($"Entregue '{dr.Value}' para '{dr.TopicPartitionOffset} | {count}'");

                        Thread.Sleep(3000);
                    }
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Entrega Falhou: {e.Error.Reason}");
                }
            }
        }
    }
}
