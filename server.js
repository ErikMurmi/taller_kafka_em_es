const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'sports-group' });

const producer = kafka.producer();
app.post('/publish', async (req, res) => {
    const message = req.body;
    await producer.send({
        topic: 'science',
        messages: [
            { value: JSON.stringify(message) },
        ],
    });
    res.status(200).send('Mensaje publicado con éxito');
});

const run = async () => {
    // Consumidor
    await consumer.connect();
    await consumer.subscribe({ topic: 'sports', fromBeginning: true });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Mensaje recibido: ${message.value.toString()}`);
            // Aquí puedes añadir la lógica para enviar el mensaje a los sistemas independientes
            // Por ejemplo:
            // axios.post('http://system1.com/api', message.value.toString());
            // axios.post('http://system2.com/api', message.value.toString());
        },
    });

    // Productor
    await producer.connect();
};

run().catch(console.error);

const port = 3000;
app.listen(port, () => console.log(`Servidor corriendo en puerto ${port}`));
