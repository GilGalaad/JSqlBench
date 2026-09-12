package engine;

import engine.dto.BenchConf;
import engine.dto.WorkerContext;
import engine.dto.WorkerResult;
import engine.strategy.DatabaseStrategy;
import engine.strategy.OracleStrategy;
import engine.strategy.PostgresStrategy;
import engine.dto.LatencyMetrics;
import lombok.extern.log4j.Log4j2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static engine.dto.WorkerStatus.KO;
import static engine.utils.CommonUtils.smartElapsed;

@Log4j2
public class BenchEngine {

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
            Thread.sleep(5000);

            // preparing threads
            ExecutorService tPool = Executors.newFixedThreadPool(conf.concurrency() + 1);
            ArrayList<Callable<WorkerResult>> callables = new ArrayList<>(conf.concurrency() + 1);
            List<Future<WorkerResult>> results;
            ArrayList<WorkerContext> workerContexts = new ArrayList<>(conf.concurrency());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(conf.time());
            for (int i = 0; i < conf.concurrency(); i++) {
                WorkerContext workerContext = new WorkerContext(i);
                workerContexts.add(workerContext);
                callables.add(new DatabaseWorker(conf, str, deadline, workerContext));
            }
            callables.add(new ProgressWorker(conf, deadline, workerContexts));

            // launching threads
            log.info("*** STARTING BENCHMARK ***");
            log.info("Starting {} concurrent threads...", conf.concurrency());
            long startTime = System.nanoTime();
            results = tPool.invokeAll(callables);
            tPool.shutdown();
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
            for (int i = 0; i < workerContexts.size(); i++) {
                WorkerResult result = results.get(i).get();
                if (result.status() == KO) {
                    log.error("Database worker {} reported exception: {}", workerContexts.get(i).workerId(), result.exception().getMessage());
                    workerFailed = true;
                }
            }

            WorkerResult progressResult = results.get(workerContexts.size()).get();
            if (progressResult.status() == KO) {
                log.error("Progress worker reported exception: {}", progressResult.exception().getMessage());
                workerFailed = true;
            }
            if (workerFailed) {
                throw new RuntimeException("Benchmark failed because one or more workers reported an exception");
            }

            ArrayList<ArrayList<Long>> samples = new ArrayList<>(workerContexts.size());
            for (WorkerContext workerContext : workerContexts) {
                samples.add(workerContext.samples());
            }
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
        } catch (ExecutionException ex) {
            throw new RuntimeException("Unexpected worker exception", ex.getCause());
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
