import java.util.*;
import java.time.Duration;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.*;

public class CombineKStream {
    final static String URL = "192.168.42.182:9092,192.168.42.63:9092,192.168.42.101:9092"; // TODO: replace by your Kafka address
    final static String AIRNULL = "---,---,---,---,---,---,---,---,---,---,---,---";
    final static String EARTHNULL = "---,---,---,---,---,---,---,---,---";
    final static String WATERNULL = "---,---,---,---,---";

    public static void main(String args[]) {
        try {
            Properties conf = new Properties();
            conf.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-app");
            conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
            conf.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
            conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> air_stream = builder.stream("air-impute");
            KStream<String, String> earth_stream = builder.stream("earth-impute");
            KStream<String, String> water_stream = builder.stream("water-impute");

            System.out.println("Starting the application to join data...");

            KStream<String, String> joined_stream = air_stream
            .outerJoin(
                earth_stream,
                (air, earth) -> (air != null ? air : AIRNULL) + "," + (earth != null ? earth : EARTHNULL),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
            )
            .outerJoin(
                water_stream,
                (combined, water) -> combined + "," + (water != null ? water : WATERNULL),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
            );

            joined_stream.foreach((key, value) -> {
                System.out.println("Joined Record - Key: " + key + "; Value: " + value);
            });
            joined_stream.to("environment", Produced.with(Serdes.String(), Serdes.String()));

            System.out.println("Begin polling...");
            KafkaStreams streams = new KafkaStreams(builder.build(), conf);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println("Shutting down... ");
                    streams.close();
                }
            });
            streams.start();
        } catch (Exception e) {
            System.err.println("Error while streaming");
            e.printStackTrace();
        }
    }
}