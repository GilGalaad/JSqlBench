package engine.utils;

import java.util.ArrayList;
import lombok.Getter;

public class MetricProvider {

    private final ArrayList<Double> timings;

    @Getter
    private final int count;
    @Getter
    private final double sum;
    @Getter
    private final double mean;
    @Getter
    private final double variance;
    @Getter
    private final double stddev;

    public MetricProvider(ArrayList<Double> timings) {
        this.timings = timings;
        this.count = timings.size();
        this.sum = calcSum();
        this.mean = calcMean();
        this.variance = calcVariance();
        this.stddev = Math.sqrt(variance);
    }

    private double calcSum() {
        double _sum = 0d;
        for (double i : timings) {
            _sum += i;
        }
        return _sum;
    }

    private double calcMean() {
        return sum / ((double) timings.size());
    }

    private double calcVariance() {
        double sum2 = 0d;
        for (double i : timings) {
            sum2 += (i - mean) * (i - mean);
        }
        return sum2 / ((double) (timings.size() - 1));
    }

}
