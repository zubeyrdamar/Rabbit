using RabbitMQ.Client;
using System.Text;

namespace RabbitMQDemo.Publisher
{
    public class Program
    {
        static void Main(string[] args)
        {
            // create a rabbit connection factory
            var factory = new ConnectionFactory
            {
                // get url from rabbit AMQP details
                Uri = new Uri("amqps://rgfzophe:BrqAwSzj0yCng9y46JS1gWa05ZXplX2e@rattlesnake.rmq.cloudamqp.com/rgfzophe")
            };

            // open a connection from the url
            using var connection = factory.CreateConnection();

            // create a channel
            var channel = connection.CreateModel();

            /*
            |
            |   At this point we are ready to communicate 
            |   with rabbit
            |
            */

            CreateQueueWithFanoutExchange(channel);
        }

        /*
        | 
        | DEFAULT EXCHANGE
        | 
        */

        public static void CreateQueueWithDefaultExchange(IModel channel)
        {
            // create a queue
            channel.QueueDeclare("hello-queue", true, false, false);

            foreach (var item in Enumerable.Range(1, 50))
            {
                // set message
                string message = $"Message {item}";
                var messageBody = Encoding.UTF8.GetBytes(message);

                // print message to queue
                channel.BasicPublish(string.Empty, "hello-queue", null, messageBody);

                Console.WriteLine($"Message has been added to queue. Message: {message}");
            }
        }

        /*
        | 
        | FANOUT EXCHANGE
        | 
        */

        public static void CreateQueueWithFanoutExchange(IModel channel)
        {
            // create a fanout exchange
            channel.ExchangeDeclare("logs-fanout", durable: true, type: ExchangeType.Fanout);

            foreach (var item in Enumerable.Range(1, 50))
            {
                // set message
                string message = $"log {item}";
                var messageBody = Encoding.UTF8.GetBytes(message);

                /*
                |
                |   At this point message is not printed to queue directly.
                |   Instead, it is sent to exchange.
                |   Any queue from consumer will create a queue and get this message.
                |
                */

                channel.BasicPublish("logs-fanout", string.Empty, null, messageBody);

                Console.WriteLine($"Message has been added to queue. Message: {message}");
            }
        }
    }
}
