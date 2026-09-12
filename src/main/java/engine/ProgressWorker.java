package engine;

import engine.dto.BenchConf;
import engine.dto.LatencyMetrics;
import engine.dto.WorkerContext;
import engine.dto.WorkerResult;
import lombok.extern.log4j.Log4j2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static engine.dto.WorkerStatus.OK;

@Log4j2
public class ProgressWorker implements Callable<WorkerResult> {

    private static final Duration INTERVAL = Duration.ofMinutes(1);

    private final BenchConf conf;
    private final long deadline;
    private final List<WorkerContext> workerContexts;
    private final ArrayList<ArrayList<Long>> samples;

    public ProgressWorker(BenchConf conf, long deadline, List<WorkerContext> workerContexts) {
        this.conf = conf;
        this.deadline = deadline;
        this.workerContexts = workerContexts;

        samples = new ArrayList<>(workerContexts.size());
        for (int i = 0; i < workerContexts.size(); i++) {
            samples.add(new ArrayList<>());
        }
    }

    @Override
    public WorkerResult call() throws InterruptedException {
        // entering loop
        while (deadline - System.nanoTime() > INTERVAL.toNanos()) {
            Thread.sleep(INTERVAL);
            // calculating partial stats
            copyNewSamples();
            LatencyMetrics metrics = LatencyMetrics.from(samples);
            long totalTransactions = metrics.count();
            if (totalTransactions == 0L) {
                log.info("Partial results: no transactions processed");
                continue;
            }
            long totalTransactionTimeNanos = metrics.sum();
            double latencyDerivedTps = (double) totalTransactions / (totalTransactionTimeNanos / 1_000_000_000d / (double) conf.concurrency());
            double averageLatency = metrics.mean() / 1_000_000d;
            double stdDev = metrics.stddev() / 1_000_000d;
            log.info("Partial results: {} tps, {} ms latency, {} stddev",
                    BigDecimal.valueOf(latencyDerivedTps).setScale(3, RoundingMode.HALF_UP),
                    BigDecimal.valueOf(averageLatency).setScale(3, RoundingMode.HALF_UP),
                    BigDecimal.valueOf(stdDev).setScale(3, RoundingMode.HALF_UP));
        }
        return new WorkerResult(OK, null);
    }

    private void copyNewSamples() {
        for (int i = 0; i < workerContexts.size(); i++) {
            ArrayList<Long> workerSamples = samples.get(i);
            workerSamples.addAll(workerContexts.get(i).samplesFrom(workerSamples.size()));
        }
    }

}
