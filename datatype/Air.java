package datatype;

import java.util.Date;

public class Air implements Environment {
    // TODO: declare all attributes
    public String type;
    public Date time;
    public String station;
    public float temperature;
    public float moisture;
    public int light;
    public float totalRainfall;
    public float rainfall;
    public int windDirection;
    public float PM2_5;
    public float PM10;
    public int CO;
    public float NOx;
    public float SO2;

    public Air() {
        this.type = "air";
        this.time = new Date();
        this.station = "unknown";
        this.temperature = 0.0f;
        this.moisture = 0.0f;
        this.light = 0;
        this.totalRainfall = 0.0f;
        this.rainfall = 0.0f;
        this.windDirection = 0;
        this.PM2_5 = 0.0f;
        this.PM10 = 0.0f;
        this.CO = 0;
        this.NOx = 0.0f;
        this.SO2 = 0.0f;
    }

    public Air(String type, Date time, String station, float temperature, float moisture, int light,
               float totalRainfall, float rainfall, int windDirection, float PM2_5, float PM10, int CO,
               float NOx, float SO2) {
        this.type = type;
        this.time = time;
        this.station = station;
        this.temperature = temperature;
        this.moisture = moisture;
        this.light = light;
        this.totalRainfall = totalRainfall;
        this.rainfall = rainfall;
        this.windDirection = windDirection;
        this.PM2_5 = PM2_5;
        this.PM10 = PM10;
        this.CO = CO;
        this.NOx = NOx;
        this.SO2 = SO2;
    }

    @Override
    public String getType() { return type; }

    @Override
    public Date getTime() { return time; }

    @Override
    public String getStation() { return station; }

    @Override
    public String toStr() {
        // TODO: Convert to string
        String result = type + "{" +
            "time=" + time +
            ",station='" + station +
            ",temperature=" + temperature +
            ",moisture=" + moisture +
            ",light=" + light +
            ",totalRainfall=" + totalRainfall +
            ",rainfall=" + rainfall +
            ",windDirection=" + windDirection +
            ",PM2.5=" + PM2_5 +
            ",PM10=" + PM10 +
            ",CO=" + CO +
            ",NOx=" + NOx +
            ",SO2=" + SO2 +
            '}';
        return result;
    }

    @Override
    public String toCSV() {
        String result =
            station + "," +
            temperature + "," +
            moisture + "," +
            light + "," +
            totalRainfall + "," +
            rainfall + "," +
            windDirection + "," +
            PM2_5 + "," +
            PM10 + "," +
            CO + "," +
            NOx + "," +
            SO2;
        return result;
    }
}
