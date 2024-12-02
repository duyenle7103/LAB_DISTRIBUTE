import java.util.Date;
import java.util.Random;
import java.util.Scanner;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import custom.EnvPartitioner;
import custom.EnvSerializer;
import datatype.Air;
import datatype.Environment;
import datatype.Earth;
import datatype.Water;

public class Producer {
    final static String URL = "192.168.42.182:9092,192.168.42.63:9092,192.168.42.101:9092"; // TODO: replace by your Kafka address
    final static SimpleDateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public static void main(String[] args) throws Exception {
        final var conf = new Properties();
        conf.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "java-producer");
        conf.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
        conf.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        conf.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EnvSerializer.class.getName());
        conf.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, EnvPartitioner.class.getName());

        KafkaProducer<String, Environment> producer = new KafkaProducer<>(conf);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down producer...");
                producer.flush();
                producer.close();
            }
        });

        System.out.println("Begin producing...");
        // TODO: read all files from dataset send them CONCURRENTLY

        // Create a thread pool for concurrent file reading
        ExecutorService executor = Executors.newFixedThreadPool(3);

        // Define the dataset folder
        File datasetFolder = new File("dataset");

        // Read files concurrently for Air, Earth, and Water
        executor.submit(() -> readAndSendData(producer, "air", datasetFolder, "AIR", Air.class));
        executor.submit(() -> readAndSendData(producer, "earth", datasetFolder, "EARTH", Earth.class));
        executor.submit(() -> readAndSendData(producer, "water", datasetFolder, "WATER", Water.class));

        // Shutdown the executor service once done
        executor.shutdown();
    }

    private static void readAndSendData(KafkaProducer<String, Environment> producer, String topic, File datasetFolder, String prefix, Class<?> dataType) {
        // List all files in the dataset folder
        File[] files = datasetFolder.listFiles((dir, name) -> name.startsWith(prefix) && name.endsWith(".csv"));

        if (files != null) {
            for (File file : files) {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        Environment data = parseData(line, dataType);
                        if (data != null) {
                            ProducerRecord<String, Environment> record = new ProducerRecord<>(topic, data);
                            producer.send(record);
                            System.out.println("Sent " + dataType.getSimpleName() + " data: " + data.toStr());
                        }
                    }
                    producer.flush();
                } catch (IOException e) {
                    System.err.println("Failed to read file " + file.getName() + ": " + e.getMessage());
                }
            }
        }
    }

    private static Environment parseData(String line, Class<?> dataType) {
        String[] fields = line.split(",");
        try {
            if (fields[0].equalsIgnoreCase("Time")) {
                return null;
            }

            for (int i = 0; i < fields.length; i++) {
                if (fields[i].equals("---")) {
                    fields[i] = "-1";
                }
            }

            Date time = DF.parse(fields[0]);
            String station = fields[1];

            if (dataType == Air.class) {
                return new Air("air", time, station, Float.parseFloat(fields[2]), Float.parseFloat(fields[3]),
                    Integer.parseInt(fields[4]), Float.parseFloat(fields[5]), Float.parseFloat(fields[6]),
                    Integer.parseInt(fields[7]), Float.parseFloat(fields[8]), Float.parseFloat(fields[9]),
                    Integer.parseInt(fields[10]), Float.parseFloat(fields[11]), Float.parseFloat(fields[12]));
            } else if (dataType == Earth.class) {
                return new Earth("earth", time, station, Float.parseFloat(fields[2]), Float.parseFloat(fields[3]),
                    Float.parseFloat(fields[4]), Float.parseFloat(fields[5]), Integer.parseInt(fields[6]),
                    Integer.parseInt(fields[7]), Integer.parseInt(fields[8]), Float.parseFloat(fields[9]));
            } else if (dataType == Water.class) {
                return new Water("water", time, station, Float.parseFloat(fields[2]), Float.parseFloat(fields[3]),
                    Float.parseFloat(fields[4]), Float.parseFloat(fields[5]));
            }
        } catch (Exception e) {
            System.err.println("Error parsing line: " + line + " - " + e.getMessage());
        }
        return null;
    }
}