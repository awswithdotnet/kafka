using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;


namespace KafkaConsumer
{

    public class KafkaMessageConsumer
    {

        private readonly string _bootstrapServers;
        private readonly string _groupId;
        private readonly AutoOffsetReset _autoOffsetReset;
        private readonly List<String> _topics;
        private bool _isConsuming;
        private readonly CancellationTokenSource _cts;
        private IConsumer<String, String> _consumer;

        public KafkaMessageConsumer()
        {            
            _bootstrapServers = "";
            _groupId = "dotnet-kafka-client";
            _autoOffsetReset = AutoOffsetReset.Earliest;
            _topics = new List<String>() { "test-topic" };
            _cts = new CancellationTokenSource();
            Console.CancelKeyPress += new ConsoleCancelEventHandler(CancelKeyPressHandler);
        }

        protected void CancelKeyPressHandler(object sender, ConsoleCancelEventArgs args){
            args.Cancel = true;
             _isConsuming = false;
            _cts.Cancel();            
        }

        public void Consume()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _bootstrapServers,
                GroupId = _groupId,
                AutoOffsetReset = (AutoOffsetReset)_autoOffsetReset
            };
                       
            try
            { 
                _consumer = new ConsumerBuilder<String, String>(config).Build();

                _consumer.Subscribe(_topics);

                _isConsuming = true;

                int i = 0;

                while (_isConsuming)
                {
                    i++;

                    Console.WriteLine(i + ": ");

                    ConsumeResult<String, String> consumeResult = _consumer.Consume(_cts.Token);

                    Console.WriteLine(consumeResult.Message.Value);


                }
            }
            catch (OperationCanceledException ex)
            {
                Console.WriteLine("Application was ended: " + ex.Message.ToString());
            }
            catch(Exception ex){
                Console.WriteLine("Application Crashed: " + ex.Message);
            }
            finally
            {                                                               
                if (_consumer != null){
                    _consumer.Close();
                    ((IDisposable)_consumer).Dispose();
                }
            }

        }

    }

}