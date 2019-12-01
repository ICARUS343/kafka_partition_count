
const kafka = require('kafka-node');

const config  = require('./config');

const Producer = kafka.HighLevelProducer;
const client = new kafka.KafkaClient({kafkaHost: config.kafka_server});
const producer = new Producer(client);
const KeyedMessage = kafka.KeyedMessage;


const pushDataToKafka =(dataToPush) => {

    try {
        let a;

        for (a = 1; a < dataToPush; a++) {
            const keyed = new KeyedMessage(a, "Node "+a);
            let payloadToKafkaTopic = [{topic: config.kafka_topic, messages: keyed}];
            producer.send(payloadToKafkaTopic, (err, data) => {

            });

        }
    }
    catch(error) {
        console.log(error);
    }

};


producer.on('error', function (err) {
    //  handle error cases here
})

producer.on('ready',  function () {
  pushDataToKafka(100);



})
