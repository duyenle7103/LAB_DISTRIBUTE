package datatype;

import java.util.Date;

public class Water implements Environment {
    // TODO: declare all attributes
    public String type;
    public Date time;
    public String station;
    public float pH;
    public float DO;
    public float temperature;
    public float salinity;

    public Water() {
        this.type = "water";
        this.time = new Date();
        this.station = "unknown";
        this.pH = 0.0f;
        this.DO = 0.0f;
        this.temperature = 0.0f;
        this.salinity = 0.0f;
    }

    public Water(String type, Date time, String station, float pH, float DO, float temperature, float salinity) {
        this.type = type;
        this.time = time;
        this.station = station;
        this.pH = pH;
        this.DO = DO;
        this.temperature = temperature;
        this.salinity = salinity;
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
                ",pH=" + pH +
                ",DO=" + DO +
                ",temperature=" + temperature +
                ",salinity=" + salinity +
                '}';
        return result;
    }

    @Override
    public String toCSV() {
        String result =
            time + "," +
            station + "," +
            pH + "," +
            DO + "," +
            temperature + "," +
            salinity;
        return result;
    }
}
