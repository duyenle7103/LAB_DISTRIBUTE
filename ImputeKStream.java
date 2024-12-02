import java.util.*;
import java.text.SimpleDateFormat;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.*;

import custom.EnvSerde;
import datatype.*;

public class ImputeKStream {
    final static String URL = "192.168.42.182:9092,192.168.42.63:9092,192.168.42.101:9092"; // TODO: replace by your Kafka address
    final String ENCODING = "UTF8";
    final static SimpleDateFormat DF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    public static void main(String args[]) {
        try {
            Properties conf = new Properties();
            conf.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app");
            conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, URL);
            conf.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
            conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EnvSerde.class.getName());

            Manager manager = new Manager();
            final Manager.AirManager airManager = (args[0].equals("air")) ? manager.getAirManager() : null;
            final Manager.EarthManager earthManager = (args[0].equals("earth")) ? manager.getEarthManager() : null;
            final Manager.WaterManager waterManager = (args[0].equals("water")) ? manager.getWaterManager() : null;

            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, Environment> env_stream = builder.stream(args[0]);

            System.out.println("Starting the application...");
            System.out.println("Topic: " + args[0]);

            KStream<String, String> imputed_stream = env_stream
            .map((key, value) -> {
                String newKey = null;
                String result = "Unknown";
                if (value instanceof Air) {
                    Air air = (Air) value;
                    imputeAirData(air, airManager);
                    newKey = DF.format(air.time);
                    result = air.toCSV();
                } else if (value instanceof Earth) {
                    Earth earth = (Earth) value;
                    imputeEarthData(earth, earthManager);
                    newKey = DF.format(earth.time);
                    result = earth.toCSV();
                } else if (value instanceof Water) {
                    Water water = (Water) value;
                    imputeWaterData(water, waterManager);
                    newKey = DF.format(water.time);
                    result = water.toCSV();
                }
                if (newKey != null) {
                    System.out.println("New Key: " + newKey + "; Value: " + result);
                    return KeyValue.pair(newKey, result);
                } else {
                    return null;
                }
            })
            .filter((key, value) -> key != null && !value.equals("Unknown"));
            imputed_stream.to(args[0] + "-impute", Produced.with(Serdes.String(), Serdes.String()));

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
            System.err.println("Error while streaming from topic: " + args[0]);
            e.printStackTrace();
        }
    }

    private static void imputeAirData(Air air, Manager.AirManager airManager) {
        if (air.temperature == -1) {
            air.temperature = airManager.airTemperatureManager.calculateNewValue();
        }
        airManager.airTemperatureManager.addValue(air.temperature);
        if (air.moisture == -1) {
            air.moisture = airManager.airMoistureManager.calculateNewValue();
        }
        airManager.airMoistureManager.addValue(air.moisture);
        if (air.light == -1) {
            air.light = (int) airManager.airLightManager.calculateNewValue();
        }
        airManager.airLightManager.addValue(air.light);
        if (air.totalRainfall == -1) {
            air.totalRainfall = airManager.airTotalRainfallManager.calculateNewValue();
        }
        airManager.airRainfallManager.addValue(air.totalRainfall);
        if (air.rainfall == -1) {
            air.rainfall = (int) airManager.airRainfallManager.calculateNewValue();
        }
        airManager.airRainfallManager.addValue(air.rainfall);
        if (air.windDirection == -1) {
            air.windDirection = (int) airManager.airWindDirectionManager.calculateNewValue();
        }
        airManager.airWindDirectionManager.addValue(air.windDirection);
        if (air.PM2_5 == -1) {
            air.PM2_5 = airManager.airPM2_5Manager.calculateNewValue();
        }
        airManager.airPM2_5Manager.addValue(air.PM2_5);
        if (air.PM10 == -1) {
            air.PM10 = airManager.airPM10Manager.calculateNewValue();
        }
        airManager.airPM10Manager.addValue(air.PM10);
        if (air.CO == -1) {
            air.CO = (int) airManager.airCOManager.calculateNewValue();
        }
        airManager.airCOManager.addValue(air.CO);
        if (air.NOx == -1) {
            air.NOx = (int) airManager.airNOxManager.calculateNewValue();
        }
        airManager.airNOxManager.addValue(air.NOx);
        if (air.SO2 == -1) {
            air.SO2 = (int) airManager.airSO2Manager.calculateNewValue();
        }
        airManager.airSO2Manager.addValue(air.SO2);
    }

    private static void imputeEarthData(Earth earth, Manager.EarthManager earthManager) {
        if (earth.moisture == -1) {
            earth.moisture = earthManager.earthMoistureManager.calculateNewValue();
        }
        earthManager.earthMoistureManager.addValue(earth.moisture);
        if (earth.temperature == -1) {
            earth.temperature = earthManager.earthTemperatureManager.calculateNewValue();
        }
        earthManager.earthTemperatureManager.addValue(earth.temperature);
        if (earth.salinity == -1) {
            earth.salinity = earthManager.earthSalinityManager.calculateNewValue();
        }
        earthManager.earthSalinityManager.addValue(earth.salinity);
        if (earth.pH == -1) {
            earth.pH = earthManager.earthPHManager.calculateNewValue();
        }
        earthManager.earthPHManager.addValue(earth.pH);
        if (earth.waterRoot == -1) {
            earth.waterRoot = (int) earthManager.earthWaterRootManager.calculateNewValue();
        }
        earthManager.earthWaterRootManager.addValue(earth.waterRoot);
        if (earth.waterLeaf == -1) {
            earth.waterLeaf = (int) earthManager.earthWaterLeafManager.calculateNewValue();
        }
        earthManager.earthWaterLeafManager.addValue(earth.waterLeaf);
        if (earth.waterLevel == -1) {
            earth.waterLevel = (int) earthManager.earthWaterLevelManager.calculateNewValue();
        }
        earthManager.earthWaterLevelManager.addValue(earth.waterLevel);
        if (earth.voltage == -1) {
            earth.voltage = earthManager.earthVoltageManager.calculateNewValue();
        }
        earthManager.earthVoltageManager.addValue(earth.voltage);
    }

    private static void imputeWaterData(Water water, Manager.WaterManager waterManager) {
        if (water.pH == -1) {
            water.pH = waterManager.waterPHManager.calculateNewValue();
        }
        waterManager.waterPHManager.addValue(water.pH);
        if (water.DO == -1) {
            water.DO = waterManager.waterDOManager.calculateNewValue();
        }
        waterManager.waterDOManager.addValue(water.DO);
        if (water.temperature == -1) {
            water.temperature = waterManager.waterTemperatureManager.calculateNewValue();
        }
        waterManager.waterTemperatureManager.addValue(water.temperature);
        if (water.salinity == -1) {
            water.salinity = waterManager.waterSalinityManager.calculateNewValue();
        }
        waterManager.waterSalinityManager.addValue(water.salinity);
    }
}