package clients;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BasicConsumer {

    public static void main(String[] args) {
        System.out.println("*** Starting Basic Consumer ***");
        Properties settings = new Properties();
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "basic-consumer");
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //settings.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        boolean flagSeekToBeginning = false;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings)) {
            consumer.subscribe(Arrays.asList("hello-world-topic"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                //System.out.println(Instant.now());
                if (flagSeekToBeginning && consumer.assignment().size() != 0) {
                    consumer.seekToBeginning(consumer.assignment());
                    flagSeekToBeginning = false;
                }

                for (ConsumerRecord<String, String> record : records) {
                    if (record.offset() %100 == 0)
                        System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n", record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }// end main method
}
