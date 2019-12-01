import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class PartitionTest {
    //vars used by both producer and consumer
    private final static String TOPIC = "new_topic_1";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";


    private static Producer<Long, String> createProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //acks -> how many acknowledgements does the producer wait 0, 1 or all
        props.put("acks", "all");
        return new KafkaProducer<>(props);
    }

    private static void runProducer(int num) throws ExecutionException, InterruptedException {
        final Producer<Long, String> producer = createProducer();

        for (long i = 0; i< num; i++){

            final ProducerRecord<Long, String> record =
                    new ProducerRecord<>(TOPIC,i,
                            "Record num:c " + i);

            RecordMetadata metadata = producer.send(record).get();

            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d)\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset());

        }
        producer.flush();
        producer.close();
    }

    private static Consumer<Long, String> createConsumer(){

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;

    }

    public static void runConsumer() {
        final Consumer<Long, String> consumer = createConsumer();
        final HashMap<Integer, Integer> partitionCounter = new HashMap<>();

        final int giveUp = 100;
        int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(10);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {

                System.out.printf("Consumer Record Received:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
                int partition = record.partition();
                if (partitionCounter.containsKey(partition)) {
                    partitionCounter.put(partition, partitionCounter.get(partition) + 1);
                } else {
                    partitionCounter.put(partition, partition);
                }
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
        System.out.println(partitionCounter);
    }




    public static void main(String[] args) throws ExecutionException, InterruptedException {
    runProducer(500);
    runConsumer();
    }
}
