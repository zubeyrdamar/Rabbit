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

            CreateQueueWithHeaderExchange(channel);
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

        /*
        | 
        | DIRECT EXCHANGE
        | 
        */
        
        public static void CreateQueueWithDirectExchange(IModel channel)
        {
            // create a direct exchange
            channel.ExchangeDeclare("logs-direct", durable: true, type: ExchangeType.Direct);

            Enum.GetNames(typeof(LogNames)).ToList().ForEach(logName =>
            {
                // create a route key
                var routeKey = $"route-{logName}";

                // create queue name for each log name
                var queueName = $"direct-queue-{logName}";

                // create queue for each log name
                channel.QueueDeclare(queueName, true, false, false);

                // bind a queue
                channel.QueueBind(queueName, "logs-direct", routeKey, null);
            });

            foreach (var item in Enumerable.Range(1, 50))
            {
                // get a random log name
                LogNames log = (LogNames) new Random().Next(1, 5);

                // create a route key
                var routeKey = $"route-{log}";

                // set message
                string message = $"log-type: {log}";
                var messageBody = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("logs-direct", routeKey, null, messageBody);

                Console.WriteLine($"Message has been added to queue. Message: {message}");
            }
        }

        /*
        | 
        | TOPIC EXCHANGE
        | 
        */

        public static void CreateQueueWithTopicExchange(IModel channel)
        {
            // create a topic exchange
            channel.ExchangeDeclare("logs-topic", durable: true, type: ExchangeType.Topic);

            Random random = new Random();
            foreach (var item in Enumerable.Range(1, 50))
            {
                LogNames log1 = (LogNames)random.Next(1, 5);
                LogNames log2 = (LogNames)random.Next(1, 5);
                LogNames log3 = (LogNames)random.Next(1, 5);

                // create a route key
                var routeKey = $"{log1}.{log2}.{log3}";

                // set message
                string message = $"log-topic: {log1}-{log2}-{log3}";
                var messageBody = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish("logs-topic", routeKey, null, messageBody);

                Console.WriteLine($"Message has been added to queue. Message: {message}");
            }
        }

        /*
        | 
        | HEADER EXCHANGE
        | 
        */

        public static void CreateQueueWithHeaderExchange(IModel channel)
        {
            // create a header exchange
            channel.ExchangeDeclare("header-exchange", durable: true, type: ExchangeType.Headers);

            // create headers
            Dictionary<string, object> headers = new Dictionary<string, object>();
            headers.Add("format", "pdf");
            headers.Add("shape", "a4");

            // create properties
            var properties = channel.CreateBasicProperties();
            properties.Headers = headers;

            var your_message = Encoding.UTF8.GetBytes("Header exchange message");
            channel.BasicPublish("header-exchange", string.Empty, properties, your_message);
        }

        public enum LogNames
        {
            Critical = 1,
            Error = 2,
            Warning = 3,
            Information = 4,
        }
    }
}
