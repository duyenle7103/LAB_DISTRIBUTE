package datatype;

public class Manager {
    private AirManager airManager;
    private EarthManager earthManager;
    private WaterManager waterManager;

    public static class AirManager {
        public HistoryManager airTemperatureManager;
        public HistoryManager airMoistureManager;
        public HistoryManager airLightManager;
        public HistoryManager airTotalRainfallManager;
        public HistoryManager airRainfallManager;
        public HistoryManager airWindDirectionManager;
        public HistoryManager airPM2_5Manager;
        public HistoryManager airPM10Manager;
        public HistoryManager airCOManager;
        public HistoryManager airNOxManager;
        public HistoryManager airSO2Manager;

        public AirManager() {
            airTemperatureManager = new HistoryManager();
            airMoistureManager = new HistoryManager();
            airLightManager = new HistoryManager();
            airTotalRainfallManager = new HistoryManager();
            airRainfallManager = new HistoryManager();
            airWindDirectionManager = new HistoryManager();
            airPM2_5Manager = new HistoryManager();
            airPM10Manager = new HistoryManager();
            airCOManager = new HistoryManager();
            airNOxManager = new HistoryManager();
            airSO2Manager = new HistoryManager();
        }
    }

    public static class EarthManager {
        public HistoryManager earthMoistureManager;
        public HistoryManager earthTemperatureManager;
        public HistoryManager earthSalinityManager;
        public HistoryManager earthPHManager;
        public HistoryManager earthWaterRootManager;
        public HistoryManager earthWaterLeafManager;
        public HistoryManager earthWaterLevelManager;
        public HistoryManager earthVoltageManager;

        public EarthManager() {
            earthMoistureManager = new HistoryManager();
            earthTemperatureManager = new HistoryManager();
            earthSalinityManager = new HistoryManager();
            earthPHManager = new HistoryManager();
            earthWaterRootManager = new HistoryManager();
            earthWaterLeafManager = new HistoryManager();
            earthWaterLevelManager = new HistoryManager();
            earthVoltageManager = new HistoryManager();
        }
    }

    public static class WaterManager {
        public HistoryManager waterPHManager;
        public HistoryManager waterDOManager;
        public HistoryManager waterTemperatureManager;
        public HistoryManager waterSalinityManager;

        public WaterManager() {
            waterPHManager = new HistoryManager();
            waterDOManager = new HistoryManager();
            waterTemperatureManager = new HistoryManager();
            waterSalinityManager = new HistoryManager();
        }
    }

    public Manager() {}

    public AirManager getAirManager() {
        if (airManager == null) {
            airManager = new AirManager();
        }
        return airManager;
    }

    public EarthManager getEarthManager() {
        if (earthManager == null) {
            earthManager = new EarthManager();
        }
        return earthManager;
    }

    public WaterManager getWaterManager() {
        if (waterManager == null) {
            waterManager = new WaterManager();
        }
        return waterManager;
    }
}