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

            CreateQueueWithDefaultExchange(channel);
        }

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
    }
}
