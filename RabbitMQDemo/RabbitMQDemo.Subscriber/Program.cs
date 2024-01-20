using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbitMQDemo.Subscriber
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

            ConsumeQueueWithDefaultExchange(channel);

            Console.ReadLine();
        }

        public static void ConsumeQueueWithDefaultExchange(IModel channel)
        {
            // create a queue

            /*
            |   
            |   It is not a must to create a queue in consumer
            |   if it is known the queue exists.
            |
            |   But if it is not certain that any queue exists with
            |   the queue name given, not to take an error, it
            |   may be a good idea to create a queue.
            |
            */

            channel.QueueDeclare("hello-queue", true, false, false);

            // arrange number of operations for each queue
            channel.BasicQos(0, 1, false);

            // create a consumer
            var consumer = new EventingBasicConsumer(channel);

            // consume the queue
            channel.BasicConsume("hello-queue", false, consumer);

            // read received message
            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                var received_message = Encoding.UTF8.GetString(e.Body.ToArray());
                Console.WriteLine(received_message);

                // remove the operation from queue which has been successfully read
                channel.BasicAck(e.DeliveryTag, false);

                Thread.Sleep(1000);
            };
        }
    }
}
