package clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class ShakespeareProducer {

    private final static String INPUT_PATH_NAME = "../../../datasets/shakespeare";

    public static void main(String[] args) {
        System.out.println("%%% Starting Shakespeare Producer %%%");
        try {
            ShakespeareProducer shakespeareProducer = new ShakespeareProducer();
            shakespeareProducer.runProducer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runProducer() throws IOException {
        KafkaProducer<String, String> producer = createProducer();
        File directory = new File(INPUT_PATH_NAME);
        System.out.println(directory.getAbsolutePath());
        if (directory.isDirectory()) {
            for (File file : directory.listFiles()) {
                sendFile(file, producer);
            }
        } else {
            sendFile(directory, producer);
        }
    }

    private KafkaProducer<String, String> createProducer() {
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(settings);
        return producer;
    }

    private void sendFile(File inputFile, KafkaProducer<String, String> producer) throws IOException {
        String key = inputFile.getName().split("\\.")[0];
        for (String line : Files.readAllLines(Paths.get(inputFile.toURI()))) {
            ProducerRecord<String, String> record = new ProducerRecord<>("shakespeare_topic", key, line);
            producer.send(record);
        }
        System.out.printf("Finished producing file: %s%n", inputFile.getName());
    }
}