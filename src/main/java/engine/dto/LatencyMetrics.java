package engine.dto;

import java.util.List;
import java.util.LongSummaryStatistics;

public record LatencyMetrics(long count, long sum, double mean, double variance, double stddev) {

    public static LatencyMetrics from(List<? extends List<Long>> samples) {
        LongSummaryStatistics statistics = samples.stream()
                .flatMapToLong(workerSamples -> workerSamples.stream().mapToLong(Long::longValue))
                .summaryStatistics();
        long count = statistics.getCount();
        long sum = statistics.getSum();
        double mean = statistics.getAverage();
        double variance;
        double stddev;

        if (count > 1) {
            double sumOfSquaredDeviations = samples.stream()
                    .flatMapToLong(workerSamples -> workerSamples.stream().mapToLong(Long::longValue))
                    .mapToDouble(sample -> (sample - mean) * (sample - mean))
                    .sum();
            variance = sumOfSquaredDeviations / (double) (count - 1);
            stddev = Math.sqrt(variance);
        } else {
            variance = 0d;
            stddev = 0d;
        }
        return new LatencyMetrics(count, sum, mean, variance, stddev);
    }

}
