const { Kafka } = require('kafkajs');

// Cấu hình Kafka client
const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: ['localhost:9092'], // Địa chỉ broker
});

const producer = kafka.producer();

const produceMessages = async () => {
  await producer.connect();
  console.log('Producer connected');

  // Gửi một vài message
  for (let i = 0; i < 100; i++) {
    const message = { value: `Message ${i}` };
    console.log('Sending:', message);

    await producer.send({
      topic: 'test-topic', // Tên topic
      messages: [message],
    });
  }

  await producer.disconnect();
  console.log('Producer disconnected');
};

produceMessages().catch((err) => {
  console.error('Error in producer:', err);
});
