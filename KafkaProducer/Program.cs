using System;

namespace KafkaProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            var kafkaMessageProducer = new KafkaMessageProducer();
            kafkaMessageProducer.Produce();
        }
    }
}
