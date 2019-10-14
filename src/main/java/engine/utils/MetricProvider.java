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

        if (timings.isEmpty()) {
            count = 0;
            sum = 0d;
            mean = 0;
            variance = 0d;
            stddev = 0d;
            return;
        }

        // population size
        count = timings.size();

        // sum
        double _sum = 0d;
        for (double i : timings) {
            _sum += i;
        }
        sum = _sum;

        // mean
        mean = sum / ((double) timings.size());

        // variance and stddev, only if more than one sample
        if (count > 1) {
            double _sum2 = 0d;
            for (double i : timings) {
                _sum2 += (i - mean) * (i - mean);
            }
            variance = _sum2 / ((double) (timings.size() - 1));
            stddev = Math.sqrt(variance);
        } else {
            variance = 0d;
            stddev = 0d;
        }
    }

}
