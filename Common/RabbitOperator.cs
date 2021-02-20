using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace Common
{
    public class RabbitOperator
    {
        public void Send(string message,string queue, string exchange="", bool durable=true,bool persistent=true,string hostName="localhost", int port= 5672)
        {
            var factory = new ConnectionFactory() { HostName = hostName, Port = port };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Fanout);
                //channel.QueueDelete("task_queue");
                //durable确保队列重启存在
                channel.QueueDeclare(queue: queue,
                                     durable: durable,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                //var message = GetMessage(args);
                var body = Encoding.UTF8.GetBytes(message);

                var properties = channel.CreateBasicProperties();
                //是否消息持久化
                properties.Persistent = persistent;

                channel.BasicPublish(exchange: "",
                                     routingKey: queue,
                                     basicProperties: properties,
                                     body: body);
               // Console.WriteLine(" [x] Sent {0}", message);
            }
        }
        public void Receive(string message, string queue, string exchange = "", bool durable = true, bool autoAck = false, string hostName = "localhost", int port = 5672)
        {
            var factory = new ConnectionFactory() { HostName = "localhost", Port = 5672 };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: exchange, type: ExchangeType.Fanout);
                var queueName = channel.QueueDeclare().QueueName;
                channel.QueueBind(queue: queueName,
                              exchange: "logs",
                              routingKey: "");
                channel.QueueDeclare(queue: queue,
                                     durable: durable,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);
                //prefetchSize
                //prefetchCount：同一时间只给一个消费者，也可以说分发新消息给消费者直到它已经处理，并确认上条消息
                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    int dots = message.Split('.').Length - 1;
                    Thread.Sleep(dots * 1000);
                    Console.WriteLine(" [x] Received {0}", message);
                    // Note: it is possible to access the channel via
                    //       ((EventingBasicConsumer)sender).Model here
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };
                channel.BasicConsume(queue: queue,
                                     autoAck: autoAck,
                                     consumer: consumer);
            }
        }
    }
}
