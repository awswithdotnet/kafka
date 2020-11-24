using System;

namespace KafkaConsumer
{
    class Program
    {
        static void Main(string[] args)
        {

            var kafkaConsumer = new KafkaMessageConsumer();

            kafkaConsumer.Consume();

        }
    }
}
