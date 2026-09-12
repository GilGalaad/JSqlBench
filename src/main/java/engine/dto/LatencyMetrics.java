package engine.dto;

import java.util.List;
import java.util.LongSummaryStatistics;

public record LatencyMetrics(long count, long totalNanos, double meanNanos, double varianceNanosSquared, double standardDeviationNanos) {

    private static final double NANOS_PER_SECOND = 1_000_000_000d;
    private static final double NANOS_PER_MILLISECOND = 1_000_000d;

    public static LatencyMetrics from(List<? extends List<Long>> samples) {
        LongSummaryStatistics statistics = samples.stream()
                .flatMapToLong(workerSamples -> workerSamples.stream().mapToLong(Long::longValue))
                .summaryStatistics();
        long count = statistics.getCount();
        long totalNanos = statistics.getSum();
        double meanNanos = statistics.getAverage();
        double varianceNanosSquared;
        double standardDeviationNanos;

        if (count > 1) {
            double sumOfSquaredDeviations = samples.stream()
                    .flatMapToLong(workerSamples -> workerSamples.stream().mapToLong(Long::longValue))
                    .mapToDouble(sample -> (sample - meanNanos) * (sample - meanNanos))
                    .sum();
            varianceNanosSquared = sumOfSquaredDeviations / (double) (count - 1);
            standardDeviationNanos = Math.sqrt(varianceNanosSquared);
        } else {
            varianceNanosSquared = 0d;
            standardDeviationNanos = 0d;
        }
        return new LatencyMetrics(count, totalNanos, meanNanos, varianceNanosSquared, standardDeviationNanos);
    }

    public boolean isEmpty() {
        return count == 0L;
    }

    public double overallTps(long elapsedNanos) {
        return (double) count * NANOS_PER_SECOND / elapsedNanos;
    }

    public double latencyDerivedTps(int concurrency) {
        return (double) count * concurrency * NANOS_PER_SECOND / totalNanos;
    }

    public double averageLatencyMillis() {
        return meanNanos / NANOS_PER_MILLISECOND;
    }

    public double standardDeviationMillis() {
        return standardDeviationNanos / NANOS_PER_MILLISECOND;
    }

}
