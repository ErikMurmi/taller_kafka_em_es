const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});


const consumer = kafka.consumer({ groupId: 'science-group' });
const run = async () => {
    // Consumidor
    await consumer.connect();
    await consumer.subscribe({ topic: 'science', fromBeginning: true });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Mensaje recibido: ${message.value.toString()}`);
        },
    });

    // Productor
    //await producer.connect();
};

run().catch(console.error);

const port = 3001;
app.listen(port, () => console.log(`Servidor corriendo en puerto ${port}`));
