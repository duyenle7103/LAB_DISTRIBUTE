package custom;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.Cluster;

import datatype.Environment;

import org.apache.kafka.clients.producer.Partitioner;

public class EnvPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing, not necessary right now
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {        
        // TODO: implement the partitioner
        Environment data = (Environment) value;
        if (data.getType().equals("air")) {
            if (data.getStation().equals("SVDT1")) {
                return 0; // Partition 0 for "air" with station "SVDT1"
            } else if (data.getStation().equals("SVDT3")) {
                return 1; // Partition 1 for "air" with station "SVDT3"
            }
            return 2;
        }
        return 0;
    }

    @Override
    public void close() {
        // Do nothing, not necessary right now
    }
}
