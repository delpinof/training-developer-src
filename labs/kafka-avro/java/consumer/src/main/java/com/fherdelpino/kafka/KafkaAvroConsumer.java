package com.fherdelpino.kafka;


import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import solution.model.ShakespeareKey;
import solution.model.ShakespeareValue;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroConsumer {

    static Logger logger = LoggerFactory.getLogger(KafkaAvroConsumer.class);

    public static void main(String[] args) {
        new KafkaAvroConsumer().run();
    }

    public void run() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-avro-consumer");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Use Specific Record or else you get Avro GenericRecord.
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        // Don't commit offset, read always --from-beginning
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<ShakespeareKey, ShakespeareValue> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton("shakespeare_avro_topic"));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.close()));

        while (true) {
            ConsumerRecords<ShakespeareKey, ShakespeareValue> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record ->
                    logger.info("key: {} value: {}", record.key().getWork(), record.value().getLine())
            );
        }
    }// end run

}// end class
