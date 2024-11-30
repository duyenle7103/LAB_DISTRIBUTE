package datatype;

import java.util.Date;

public class Earth implements Environment {
    // TODO: declare all attributes
    public String type;
    public Date time;
    public String station;
    public float moisture;
    public float temperature;
    public float salinity;
    public float pH;
    public int waterRoot;
    public int waterLeaf;
    public int waterLevel;
    public float voltage;

    public Earth() {
        this.type = "earth";
        this.time = new Date();
        this.station = "unknown";
        this.moisture = 0.0f;
        this.temperature = 0.0f;
        this.salinity = 0.0f;
        this.pH = 0.0f;
        this.waterRoot = 0;
        this.waterLeaf = 0;
        this.waterLevel = 0;
        this.voltage = 0.0f;
    }

    public Earth(String type, Date time, String station, float moisture, float temperature, float salinity, float pH,
                 int waterRoot, int waterLeaf, int waterLevel, float voltage) {
        this.type = type;
        this.time = time;
        this.station = station;
        this.moisture = moisture;
        this.temperature = temperature;
        this.salinity = salinity;
        this.pH = pH;
        this.waterRoot = waterRoot;
        this.waterLeaf = waterLeaf;
        this.waterLevel = waterLevel;
        this.voltage = voltage;
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
                ",moisture=" + moisture +
                ",temperature=" + temperature +
                ",salinity=" + salinity +
                ",pH=" + pH +
                ",waterRoot=" + waterRoot +
                ",waterLeaf=" + waterLeaf +
                ",waterLevel=" + waterLevel +
                ",voltage=" + voltage +
                '}';
        return result;
    }

    @Override
    public String toCSV() {
        String result =
            time + "," +
            station + "," +
            moisture + "," +
            temperature + "," +
            salinity + "," +
            pH + "," +
            waterRoot + "," +
            waterLeaf + "," +
            waterLevel + "," +
            voltage;
        return result;
    }
}
