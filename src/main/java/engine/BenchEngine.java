package engine;

import engine.dto.BenchConf;
import engine.dto.LatencyMetrics;
import engine.dto.LatencyPercentiles;
import engine.dto.WorkerResult;
import engine.strategy.DatabaseStrategy;
import engine.strategy.OracleStrategy;
import engine.strategy.PostgresStrategy;
import lombok.extern.log4j.Log4j2;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static engine.dto.WorkerStatus.KO;
import static engine.utils.CommonUtils.roundMetric;
import static engine.utils.CommonUtils.smartElapsed;

@Log4j2
public class BenchEngine {

    private static final Duration SETTLE_TIME = Duration.ofSeconds(5);

    private final BenchConf conf;
    private final DatabaseStrategy str;

    public BenchEngine(BenchConf conf) throws UnsupportedOperationException {
        this.conf = conf;
        try {
            str = switch (conf.engine()) {
                case ORACLE -> new OracleStrategy(conf);
                case POSTGRES -> new PostgresStrategy(conf);
            };
        } catch (ClassNotFoundException ex) {
            throw new UnsupportedOperationException("Error while initializing engine, database driver not found");
        }
    }

    public void run() {
        try {
            log.info("*** PREPARING FOR BENCHMARK ***");
            try {
                prepareDatabase();
            } catch (SQLException ex) {
                throw new RuntimeException("Error while preparing database for benchmark: " + ex.getMessage(), ex);
            }

            // let settle down a bit
            Thread.sleep(SETTLE_TIME);

            // preparing threads
            ArrayList<Callable<WorkerResult>> callables = new ArrayList<>(conf.concurrency() + 1);
            List<Future<WorkerResult>> results;
            ArrayList<WorkerContext> workerContexts = new ArrayList<>(conf.concurrency());
            long deadline = System.nanoTime() + Duration.ofSeconds(conf.time()).toNanos();
            for (int i = 0; i < conf.concurrency(); i++) {
                WorkerContext workerContext = new WorkerContext();
                workerContexts.add(workerContext);
                callables.add(new DatabaseWorker(conf, str, deadline, workerContext));
            }
            callables.add(new ProgressWorker(conf, deadline, workerContexts));

            // launching threads
            log.info("*** STARTING BENCHMARK ***");
            log.info("Starting {} concurrent threads...", conf.concurrency());
            long startTime = System.nanoTime();
            try (ExecutorService executor = Executors.newFixedThreadPool(conf.concurrency() + 1)) {
                results = executor.invokeAll(callables);
            }
            long endTime = System.nanoTime();

            try {
                cleanupDatabase();
            } catch (SQLException ex) {
                throw new RuntimeException("Error while cleaning up database: " + ex.getMessage(), ex);
            }

            // printing result
            log.info("*** BENCHMARK RESULT ***");
            log.info("Scale factor: {}", conf.scale());
            log.info("Number of concurrent clients: {}", conf.concurrency());

            long elapsedNano = endTime - startTime;
            log.info("Total time elapsed: {}", smartElapsed(elapsedNano));

            boolean workerFailed = false;
            for (int i = 0; i < results.size(); i++) {
                WorkerResult result = results.get(i).resultNow();
                if (result.status() == KO) {
                    log.error("Worker #{} reported exception: {}", i + 1, result.exception().getMessage());
                    workerFailed = true;
                }
            }
            if (workerFailed) {
                throw new RuntimeException("Benchmark failed because one or more workers reported an exception");
            }

            List<List<Long>> samples = workerContexts.stream().map(WorkerContext::allSamples).toList();
            LatencyMetrics metrics = LatencyMetrics.from(samples);
            if (metrics.isEmpty()) {
                log.info("No transaction processed, no result to show");
                return;
            }

            // calculating metrics
            long totalTransactions = metrics.count();
            log.info("Total number of transactions processed: {}", totalTransactions);

            double overallTps = metrics.overallTps(elapsedNano);
            log.info("Transactions per second: {} (overall)", roundMetric(overallTps, 1));

            double latencyDerivedTps = metrics.latencyDerivedTps(conf.concurrency());
            log.info("Transactions per second: {} (derived from client-observed transaction latency)", roundMetric(latencyDerivedTps, 1));

            double averageLatency = metrics.averageLatencyMillis();
            log.info("Average latency: {} ms", roundMetric(averageLatency, 3));

            double stdDev = metrics.standardDeviationMillis();
            log.info("Latency stddev: {}  ms", roundMetric(stdDev, 3));

            LatencyPercentiles percentiles = LatencyPercentiles.from(samples);
            log.info("Latency percentiles: p50 {} ms, p95 {} ms, p99 {} ms",
                    roundMetric(percentiles.p50Millis(), 3), roundMetric(percentiles.p95Millis(), 3), roundMetric(percentiles.p99Millis(), 3));

            double coefficientOfVariation = metrics.coefficientOfVariationPercent();
            log.info("Latency coefficient of variation: {}%", roundMetric(coefficientOfVariation, 1));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Benchmark interrupted", ex);
        }
    }

    private void prepareDatabase() throws SQLException {
        try (Connection c = str.doConnect()) {
            str.dropTables(c);
            str.createTables(c);
            str.populateTables(c);
            str.createIndexes(c);
            str.analyzeTables(c);
        }
    }

    private void cleanupDatabase() throws SQLException {
        try (Connection c = str.doConnect()) {
            str.dropTables(c);
        }
    }

}
