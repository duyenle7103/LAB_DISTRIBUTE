package custom;

import java.util.Map;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import datatype.Air;
import datatype.Environment;
import datatype.Earth;
import datatype.Water;

public class EnvDeserialzer implements Deserializer<Environment> {
    final String ENCODING = "UTF8";
    final SimpleDateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Do nothing, not necessary right now
    }

    @Override
    public Environment deserialize(String topic, byte[] data) {
        // TODO: implement the deserialize
        try {
            if (data == null) {
                return null;
            }

            ByteBuffer buf = ByteBuffer.wrap(data);

            // Deserialize `type`
            byte[] typeBytes = new byte[buf.getInt()];
            buf.get(typeBytes);
            String type = new String(typeBytes, ENCODING);

            // Deserialize `time`
            byte[] timeBytes = new byte[buf.getInt()];
            buf.get(timeBytes);
            Date time = DF.parse(new String(timeBytes, ENCODING));

            // Deserialize `station`
            byte[] stationBytes = new byte[buf.getInt()];
            buf.get(stationBytes);
            String station = new String(stationBytes, ENCODING);

            // Deserialize based on the `type` field
            if (type.equals("air")) {
                float temperature = buf.getFloat();
                float moisture = buf.getFloat();
                int light = buf.getInt();
                float totalRainfall = buf.getFloat();
                float rainfall = buf.getFloat();
                int windDirection = buf.getInt();
                float PM2_5 = buf.getFloat();
                float PM10 = buf.getFloat();
                int CO = buf.getInt();
                float NOx = buf.getFloat();
                float SO2 = buf.getFloat();

                return new Air(type, time, station, temperature, moisture, light, totalRainfall,
                               rainfall, windDirection, PM2_5, PM10, CO, NOx, SO2);

            } else if (type.equals("earth")) {
                float moisture = buf.getFloat();
                float temperature = buf.getFloat();
                float salinity = buf.getFloat();
                float pH = buf.getFloat();
                int waterRoot = buf.getInt();
                int waterLeaf = buf.getInt();
                int waterLevel = buf.getInt();
                float voltage = buf.getFloat();

                return new Earth(type, time, station, moisture, temperature, salinity, pH,
                                 waterRoot, waterLeaf, waterLevel, voltage);

            } else if (type.equals("water")) {
                float pH = buf.getFloat();
                float DO = buf.getFloat();
                float temperature = buf.getFloat();
                float salinity = buf.getFloat();

                return new Water(type, time, station, pH, DO, temperature, salinity);
            }

            // If type is unknown, return null or throw an exception
            return null;

        } catch (Exception e) {
            throw new SerializationException("Error when deserializing Environment data.", e);
        }
    }

    @Override
    public void close() {
        // Do nothing, not necessary right now
    }
}
