package clients;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ManualOffsetConsumer {

    private static final String TOPIC_NAME = "one-p-numbers-topic";
    private static final String FILE_NAME = "offset.log";
    private static long lastOffset;

    public static void main(String[] args) {
        System.out.println("*** Starting Manual offset Consumer ***");

        Properties settings = new Properties();
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "write-offset-consumer");
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        settings.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Files.write(Paths.get(FILE_NAME), String.valueOf(lastOffset).getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        String fileOffset = null;
        try {
            List<String> lines = Files.readAllLines(Paths.get(FILE_NAME));
            if (lines.size() == 1)
                fileOffset = lines.get(0);
        } catch (IOException e) {
            System.out.printf("File %s not found.%n", FILE_NAME);
        }

        boolean seekFromFile = fileOffset != null && !fileOffset.isEmpty();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings)) {
            consumer.subscribe(Arrays.asList(TOPIC_NAME));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (seekFromFile && consumer.assignment().size() != 0) {
                    for (TopicPartition tp : consumer.assignment())
                        consumer.seek(tp, Long.parseLong(fileOffset));
                    seekFromFile = false;
                }
                for (ConsumerRecord<String, String> record : records) {
                    lastOffset = record.offset();
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n", record.partition(), lastOffset, record.key(), record.value());
                }

            }
        }
    }//end main
}
