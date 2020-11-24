using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace KafkaProducer
{

    public class KafkaMessageProducer
    {
        private const int _flushTime = 20;
        private readonly string _bootstrapServers;        
        private readonly String _topic;                
        private IProducer<String, String> _producer;

        public KafkaMessageProducer()
        {            
            _bootstrapServers = "";            
            _topic = "test-topic";                        
        }        

        public void Produce()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers
            };
                       
            try
            { 
                _producer = new ProducerBuilder<String, String>(config).Build();     
                _producer.Produce(_topic, new Message<string, string> { Key = Guid.NewGuid().ToString(), Value = "New Message: " + DateTime.Now.ToString() }, deliveryReportHandler);          
                _producer.Flush(TimeSpan.FromSeconds(_flushTime));                
            }    
            catch(Exception ex){
                Console.WriteLine("Application Crashed: " + ex.Message);
            }
            finally
            {                                                               
                if (_producer != null){
                    
                    ((IDisposable)_producer).Dispose();
                }
            }

        }

        private void deliveryReportHandler(DeliveryReport<string, string> deliveryReport)
        {
            
        }
    }

}