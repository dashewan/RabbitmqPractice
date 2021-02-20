using RabbitMQ.Client;
using System;
using System.Linq;
using System.Text;

namespace TopicSend
{
    class Send
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost", Port = 5672 };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {

                var severity = (args.Length > 0) ? args[0] : "info";
                var message = (args.Length > 1)
                         ? string.Join(" ", args.Skip(1).ToArray())
                         : "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);
                channel.ExchangeDeclare(exchange: "topic_logs", type: "topic");
                channel.BasicPublish(exchange: "topic_logs",
                                     routingKey: severity,
                                     basicProperties: null,
                                     body: body);
                Console.WriteLine(" [x] Sent {0}", message);
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
