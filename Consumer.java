import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import custom.EnvDeserialzer;
import datatype.Environment;

public class Consumer {
    final static String GROUPID = "java-consumer-group";
    final static String URL = "192.168.42.182:9092,192.168.42.63:9092,192.168.42.101:9092";   // TODO: replace by your Kafka address

    public static void main(String[] args) throws Exception {
        final var conf = new Properties();
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);    
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
        conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EnvDeserialzer.class.getName());
    
        KafkaConsumer<String, Environment> consumer = new KafkaConsumer<>(conf);
        consumer.subscribe(List.of(args[0]));   // Pass topic name with command line arguments

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down...");
                consumer.close();
            }
        });
        
        System.out.println("Begin polling...");
        while (true) {
            ConsumerRecords<String, Environment> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.println(record.value().toStr());
            });
        }
    }
}
