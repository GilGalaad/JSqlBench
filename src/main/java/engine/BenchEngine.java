package engine;

import static engine.BenchResult.ExecStatus.KO;
import static engine.CommonUtils.smartElapsed;
import engine.dto.BenchConf;
import static engine.dto.BenchConf.DbEngine.ORACLE;
import static engine.dto.BenchConf.DbEngine.POSTGRES;
import engine.strategy.DatabaseStrategy;
import engine.strategy.OracleStrategy;
import engine.strategy.PostgresStrategy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class BenchEngine {

    private final BenchConf conf;
    private final DatabaseStrategy str;
    private final ArrayList<Long> timings = new ArrayList<>();
    private final Object lock = new Object();

    public BenchEngine(BenchConf conf) throws UnsupportedOperationException {
        this.conf = conf;
        try {
            if (conf.getEngine() == ORACLE) {
                str = new OracleStrategy(conf);
            } else if (conf.getEngine() == POSTGRES) {
                str = new PostgresStrategy(conf);
            } else {
                throw new AssertionError("Unreachable code branch");
            }
        } catch (ClassNotFoundException ex) {
            throw new UnsupportedOperationException("Error while initializing engine, database driver not found");
        }
    }

    public void run() {
        log.info("*** PREPARING FOR BENCHMARK ***");
        try {
            prepareDatabase();
        } catch (SQLException ex) {
            throw new RuntimeException("Error while preparing database for benchmark: " + ex.getMessage(), ex);
        }

        try {
            // let settle down a bit
            log.info("Settling down for 5 seconds...");
            Thread.sleep(5000);
            // preparing threads
            log.info("Starting {} concurrent threads...", conf.getConcurrency());
            ExecutorService tPool = Executors.newFixedThreadPool(conf.getConcurrency() + 1);
            ArrayList<Callable<BenchResult>> tList = new ArrayList<>(conf.getConcurrency() + 1);
            List<Future<BenchResult>> tRes = null;
            // calculate execution deadline
            Calendar cal = Calendar.getInstance();
            cal.setLenient(false);
            cal.add(Calendar.SECOND, conf.getTime());
            long deadline = cal.getTimeInMillis();
            for (int i = 0; i < conf.getConcurrency(); i++) {
                tList.add(new DatabaseWorker(conf, str, deadline, timings, lock));
            }
            tList.add(new ProgressWorker(conf, deadline, timings, lock));
            // launching threads
            long startTime = System.nanoTime();
            tRes = tPool.invokeAll(tList);
            tPool.shutdown();
            waitAll(tRes);
            long endTime = System.nanoTime();

            try {
                cleanupDatabase();
            } catch (SQLException ex) {
                throw new RuntimeException("Error while cleaning up database: " + ex.getMessage(), ex);
            }

            // calculating metrics
            log.info("*** BENCHMARK RESULTS ***");
            log.info("Scale factor: {}", conf.getScale());
            log.info("Number of concurrent clients: {}", conf.getConcurrency());
            long elapsedNano = endTime - startTime;
            log.info("Total time elapsed: {}", smartElapsed(elapsedNano));
            for (Future<BenchResult> f : tRes) {
                if (f.get().getStatus() == KO) {
                    log.error("Thead reported exception: {}", f.get().getEx().getMessage());
                }
            }
            long totTrans = timings.size();
            long rawTime = timings.stream().mapToLong(i -> i).sum();
            if (totTrans != 0) {
                log.info("Total number of transactions processed: {}", totTrans);
            } else {
                log.info("No transaction processed, no result to show");
                return;
            }
            double totTps = (double) totTrans * 1_000_000_000.0d / (double) elapsedNano;
            log.info("Transactions per second: {} (including connection and client overhead)", BigDecimal.valueOf(totTps).setScale(3, RoundingMode.HALF_UP));
            double rawTps = (double) totTrans * 1_000_000_000.0d / ((double) rawTime / (double) conf.getConcurrency());
            log.info("Transactions per second: {} (excluding connection and client overhead)", BigDecimal.valueOf(rawTps).setScale(3, RoundingMode.HALF_UP));
            double avgLatency = (double) rawTime / 1_000_000.0d / (double) totTrans;
            log.info("Average latency: {} ms", BigDecimal.valueOf(avgLatency).setScale(3, RoundingMode.HALF_UP));
            double stdDev = Math.sqrt((timings.stream().mapToDouble(x -> ((double) x) - avgLatency).map(x -> x * x).sum()) / ((double) totTrans)) / 1_000_000.0d;
            log.info("Latency stddev: {}  ms", BigDecimal.valueOf(stdDev).setScale(3, RoundingMode.HALF_UP));
        } catch (InterruptedException | ExecutionException ex) {
            // should never happen
            log.error("Unexpected {}: {}", ex.getClass().getSimpleName(), ex.getMessage());
        }
    }

    private void waitAll(List<Future<BenchResult>> tRes) {
        for (Future f : tRes) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException ex) {
                // do nothing, will be handled later
            }
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
