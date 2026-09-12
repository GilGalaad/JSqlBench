package engine;

import engine.dto.BenchConf;
import engine.dto.LatencyMetrics;
import engine.dto.WorkerResult;
import lombok.extern.log4j.Log4j2;

import java.time.Duration;
import java.util.ArrayList;
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
        try {
            // entering loop
            while (deadline - System.nanoTime() > INTERVAL.toNanos()) {
                Thread.sleep(INTERVAL);
                // calculating partial stats
                copyNewSamples();
                LatencyMetrics metrics = LatencyMetrics.from(samples);

                if (metrics.isEmpty()) {
                    log.info("Partial results: no transactions completed so far");
                    continue;
                }
                double latencyDerivedTps = metrics.latencyDerivedTps(conf.concurrency());
                double averageLatency = metrics.averageLatencyMillis();
                double stdDev = metrics.standardDeviationMillis();
                log.info("Partial results: {} tps, {} ms latency, {} stddev", roundMetric(latencyDerivedTps), roundMetric(averageLatency), roundMetric(stdDev));
            }
        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            return new WorkerResult(KO, ex);
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
