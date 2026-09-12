package engine;

import engine.dto.BenchConf;
import engine.dto.LatencyMetrics;
import engine.dto.LatencyPercentiles;
import engine.dto.WorkerResult;
import lombok.extern.log4j.Log4j2;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import static engine.dto.WorkerStatus.KO;
import static engine.dto.WorkerStatus.OK;
import static engine.utils.CommonUtils.roundMetric;

@Log4j2
public class ProgressWorker implements Callable<WorkerResult> {

    private static final Duration INTERVAL = Duration.ofMinutes(1);

    private final BenchConf conf;
    private final long deadline;
    private final List<WorkerContext> workerContexts;
    private final List<Integer> nextSampleIndexes;

    public ProgressWorker(BenchConf conf, long deadline, List<WorkerContext> workerContexts) {
        this.conf = conf;
        this.deadline = deadline;
        this.workerContexts = workerContexts;
        nextSampleIndexes = new ArrayList<>(Collections.nCopies(workerContexts.size(), 0));
    }

    @Override
    public WorkerResult call() throws InterruptedException {
        try {
            // entering loop
            while (deadline - System.nanoTime() > INTERVAL.toNanos()) {
                Thread.sleep(INTERVAL);
                // calculating partial stats
                List<List<Long>> samples = copyNewSamples();
                LatencyMetrics metrics = LatencyMetrics.from(samples);

                if (metrics.isEmpty()) {
                    log.info("Partial results: no transactions completed in the last interval");
                    continue;
                }
                double latencyDerivedTps = metrics.latencyDerivedTps(conf.concurrency());
                double averageLatency = metrics.averageLatencyMillis();
                double stdDev = metrics.standardDeviationMillis();
                LatencyPercentiles percentiles = LatencyPercentiles.from(samples);
                double coefficientOfVariation = metrics.coefficientOfVariationPercent();
                log.info("Partial results: {} tps, {} ms latency, {} stddev, p50 {} ms, p95 {} ms, p99 {} ms, CV {}%",
                        roundMetric(latencyDerivedTps, 1), roundMetric(averageLatency, 3), roundMetric(stdDev, 3),
                        roundMetric(percentiles.p50Millis(), 3), roundMetric(percentiles.p95Millis(), 3), roundMetric(percentiles.p99Millis(), 3),
                        roundMetric(coefficientOfVariation, 1));
            }
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            return new WorkerResult(KO, ex);
        }
        return new WorkerResult(OK, null);
    }

    private List<List<Long>> copyNewSamples() {
        List<List<Long>> samples = new ArrayList<>(workerContexts.size());
        for (int i = 0; i < workerContexts.size(); i++) {
            int fromIndex = nextSampleIndexes.get(i);
            List<Long> workerSamples = workerContexts.get(i).samplesFrom(fromIndex);
            nextSampleIndexes.set(i, fromIndex + workerSamples.size());
            samples.add(workerSamples);
        }
        return samples;
    }

}
