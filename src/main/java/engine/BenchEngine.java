package engine;

import engine.dto.BenchConf;
import engine.dto.LatencyMetrics;
import engine.dto.WorkerResult;
import engine.strategy.DatabaseStrategy;
import engine.strategy.OracleStrategy;
import engine.strategy.PostgresStrategy;
import lombok.extern.log4j.Log4j2;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
            double elapsedSec = ((double) (endTime - startTime)) / 1_000_000_000d;
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
            if (metrics.count() == 0L) {
                log.info("No transaction processed, no result to show");
                return;
            }

            // calculating metrics
            long totalTransactions = metrics.count();
            log.info("Total number of transactions processed: {}", totalTransactions);
            long totalTransactionTimeNanos = metrics.sum();

            double overallTps = (double) totalTransactions / elapsedSec;
            log.info("Transactions per second: {} (overall)", BigDecimal.valueOf(overallTps).setScale(3, RoundingMode.HALF_UP));

            double latencyDerivedTps = (double) totalTransactions / (totalTransactionTimeNanos / 1_000_000_000d / (double) conf.concurrency());
            log.info("Transactions per second: {} (derived from client-observed transaction latency)", BigDecimal.valueOf(latencyDerivedTps).setScale(3, RoundingMode.HALF_UP));

            double averageLatency = metrics.mean() / 1_000_000d;
            log.info("Average latency: {} ms", BigDecimal.valueOf(averageLatency).setScale(3, RoundingMode.HALF_UP));

            double stdDev = metrics.stddev() / 1_000_000d;
            log.info("Latency stddev: {}  ms", BigDecimal.valueOf(stdDev).setScale(3, RoundingMode.HALF_UP));
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
