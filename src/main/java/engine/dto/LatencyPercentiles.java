package engine.dto;

import java.util.List;

public record LatencyPercentiles(long p50Nanos, long p95Nanos, long p99Nanos) {

    private static final double NANOS_PER_MILLISECOND = 1_000_000d;

    public static LatencyPercentiles from(List<? extends List<Long>> samples) {
        long[] sortedSamples = samples.stream().flatMapToLong(workerSamples -> workerSamples.stream().mapToLong(Long::longValue)).sorted().toArray();
        return new LatencyPercentiles(nearestRank(sortedSamples, 50), nearestRank(sortedSamples, 95), nearestRank(sortedSamples, 99));
    }

    private static long nearestRank(long[] sortedSamples, int percentile) {
        int index = (int) (((long) percentile * sortedSamples.length + 99L) / 100L - 1L);
        return sortedSamples[index];
    }

    public double p50Millis() {
        return p50Nanos / NANOS_PER_MILLISECOND;
    }

    public double p95Millis() {
        return p95Nanos / NANOS_PER_MILLISECOND;
    }

    public double p99Millis() {
        return p99Nanos / NANOS_PER_MILLISECOND;
    }

}
