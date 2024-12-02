package custom;

import java.util.Map;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import datatype.Air;
import datatype.Environment;
import datatype.Earth;
import datatype.Water;

public class EnvSerializer implements Serializer<Environment> {
    final String ENCODING = "UTF8";
    final SimpleDateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Do nothing, not necessary right now
    }

    @Override
    public byte[] serialize(String topic, Environment data) {
        // TODO: implement the serializer
        try {
            if (data ==null) {
                return null;
            }

            // Serialize string data to byte
            byte[] type = data.getType().getBytes("UTF8");
            byte[] time = DF.format(data.getTime()).getBytes("UTF8");
            byte[] station = data.getStation().getBytes("UTF8");
            ByteBuffer buf = null;
            if (data instanceof Air) {
                Air airData = (Air)data;
                int len = Integer.BYTES + type.length + Integer.BYTES + time.length + Integer.BYTES + station.length +
                        Float.BYTES * 8 + Integer.BYTES * 3;
                buf = ByteBuffer.allocate(len);
                buf.putInt(type.length);
                buf.put(type);
                buf.putInt(time.length);
                buf.put(time);
                buf.putInt(station.length);
                buf.put(station);
                buf.putFloat(airData.temperature);
                buf.putFloat(airData.moisture);
                buf.putInt(airData.light);
                buf.putFloat(airData.totalRainfall);
                buf.putFloat(airData.rainfall);
                buf.putInt(airData.windDirection);
                buf.putFloat(airData.PM2_5);
                buf.putFloat(airData.PM10);
                buf.putInt(airData.CO);
                buf.putFloat(airData.NOx);
                buf.putFloat(airData.SO2);
            } else if (data instanceof Earth) {
                Earth earthData = (Earth)data;
                int len = Integer.BYTES + type.length + Integer.BYTES + time.length + Integer.BYTES + station.length +
                        Float.BYTES * 5 + Integer.BYTES * 3;
                buf = ByteBuffer.allocate(len);
                buf.putInt(type.length);
                buf.put(type);
                buf.putInt(time.length);
                buf.put(time);
                buf.putInt(station.length);
                buf.put(station);
                buf.putFloat(earthData.moisture);
                buf.putFloat(earthData.temperature);
                buf.putFloat(earthData.salinity);
                buf.putFloat(earthData.pH);
                buf.putInt(earthData.waterRoot);
                buf.putInt(earthData.waterLeaf);
                buf.putInt(earthData.waterLevel);
                buf.putFloat(earthData.voltage);
            } else if (data instanceof Water) {
                Water waterData = (Water)data;
                int len = Integer.BYTES + type.length + Integer.BYTES + time.length + Integer.BYTES + station.length +
                        Float.BYTES * 4;
                buf = ByteBuffer.allocate(len);
                buf.putInt(type.length);
                buf.put(type);
                buf.putInt(time.length);
                buf.put(time);
                buf.putInt(station.length);
                buf.put(station);
                buf.putFloat(waterData.pH);
                buf.putFloat(waterData.DO);
                buf.putFloat(waterData.temperature);
                buf.putFloat(waterData.salinity);
            }
            return buf != null ? buf.array() : null;
        } catch (Exception e) {
            throw new SerializationException("Error when serializing environment data.");
        }
    }

    @Override
    public void close() {
        // Do nothing, not necessary right now
    }
}
