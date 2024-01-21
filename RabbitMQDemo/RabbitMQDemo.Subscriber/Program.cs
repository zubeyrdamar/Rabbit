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

            ConsumeQueueWithHeaderExchange(channel);

            Console.ReadLine();
        }

        /*
        | 
        | DEFAULT EXCHANGE
        | 
        */

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

        /*
        | 
        | FANOUT EXCHANGE
        | 
        */

        public static void ConsumeQueueWithFanoutExchange(IModel channel)
        {
            // create a random queue name
            var queueName = channel.QueueDeclare().QueueName;

            // bind a queue
            channel.QueueBind(queueName, "logs-fanout", string.Empty, null);

            /*
            |   
            |   It is not desired to have the queue after the instance
            |   destroyed. That's why a queue is binded but not declared.
            |
            */

            // arrange number of operations for each queue
            channel.BasicQos(0, 1, false);

            // create a consumer
            var consumer = new EventingBasicConsumer(channel);

            // consume the queue
            channel.BasicConsume(queueName, false, consumer);

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

        /*
        | 
        | DIRECT EXCHANGE
        | 
        */

        public static void ConsumeQueueWithDirectExchange(IModel channel)
        {
            // arrange number of operations for each queue
            channel.BasicQos(0, 1, false);

            // create a consumer
            var consumer = new EventingBasicConsumer(channel);

            // queue name
            var queueName = "direct-queue-Critical";

            // consume the queue
            channel.BasicConsume(queueName, false, consumer);

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

        /*
        | 
        | TOPIC EXCHANGE
        | 
        */

        public static void ConsumeQueueWithTopicExchange(IModel channel)
        {
            // arrange number of operations for each queue
            channel.BasicQos(0, 1, false);

            // create a consumer
            var consumer = new EventingBasicConsumer(channel);

            // random queue name
            var queueName = channel.QueueDeclare().QueueName;

            // create a route key
            var routeKey = "*.Error.*";

            // bind a queue
            channel.QueueBind(queueName, "logs-topic", routeKey);

            // consume the queue
            channel.BasicConsume(queueName, false, consumer);

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

        /*
        | 
        | HEADER EXCHANGE
        | 
        */

        public static void ConsumeQueueWithHeaderExchange(IModel channel)
        {
            // arrange number of operations for each queue
            channel.BasicQos(0, 1, false);

            // create a consumer
            var consumer = new EventingBasicConsumer(channel);

            // random queue name
            var queueName = channel.QueueDeclare().QueueName;

            // create headers
            Dictionary<string, object> headers = new Dictionary<string, object>();
            headers.Add("format", "pdf");
            headers.Add("shape", "a4");
            headers.Add("x-match", "all");

            // bind a queue
            channel.QueueBind(queueName, "header-exchange", String.Empty, headers);

            // consume the queue
            channel.BasicConsume(queueName, false, consumer);

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
