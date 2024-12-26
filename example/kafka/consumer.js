const { Kafka } = require('kafkajs');

// Cấu hình Kafka client
const kafka = new Kafka({
  clientId: 'my-consumer',
  brokers: ['localhost:9092'], // Địa chỉ broker
});

const consumer = kafka.consumer({ groupId: 'my-group' });

const consumeMessages = async () => {
  await consumer.connect();
  console.log('Consumer connected');

  // Subcribe vào topic
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

  // Lắng nghe và xử lý message
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(
        `Received message: ${message.value.toString()} from ${topic}`
      );
    },
  });
};

consumeMessages().catch((err) => {
  console.error('Error in consumer:', err);
});
