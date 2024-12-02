package datatype;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HistoryManager {
    private List<Float> history;
    private float sum;
    private int count;
    Random rand;

    public HistoryManager() {
        this.history = new ArrayList<>();
        this.sum = 0;
        this.count = 0;
        this.rand = new Random();
    }

    public List<Float> getHistory() {
        return history;
    }

    public void addValue(float newValue) {
        history.add(newValue);
        sum += newValue;
        count++;
    }

    public float calculateNewValue() {
        if (count == 0) {
            return 0;
        }
        
        float mean = sum / count;
        float sumSquaredDifferences = 0;
        for (float value : history) {
            sumSquaredDifferences += Math.pow(value - mean, 2);
        }
        double stddev = Math.sqrt(sumSquaredDifferences / count);

        float randomOffset = (float) ((rand.nextFloat() * 2 * stddev) - stddev); // Limits within the range [-stddev, stddev]
        return mean + randomOffset;
    }
}
