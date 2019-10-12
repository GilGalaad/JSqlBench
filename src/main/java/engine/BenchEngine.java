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
        System.out.println("*** PREPARING FOR BENCHMARK ***");
        try {
            prepareDatabase();
        } catch (SQLException ex) {
            throw new RuntimeException("Error while preparing database for benchmark: " + ex.getMessage(), ex);
        }

        try {
            // let settle down a bit
            Thread.sleep(5000);
            // preparing threads
            System.out.println("Starting " + conf.getConcurrency() + " concurrent threads...");
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
            System.out.println("*** BENCHMARK RESULTS ***");
            System.out.println("Scale factor: " + conf.getScale());
            System.out.println("Number of concurrent clients: " + conf.getConcurrency());
            long elapsedNano = endTime - startTime;
            System.out.println("Total time elapsed: " + smartElapsed(elapsedNano));
            for (Future<BenchResult> f : tRes) {
                if (f.get().getStatus() == KO) {
                    System.out.println("Thead reported exception: " + f.get().getEx().getMessage());
                }
            }
            long totTrans = timings.size();
            long rawTime = timings.stream().mapToLong(i -> i).sum();
            if (totTrans != 0) {
                System.out.println("Total number of transactions processed: " + totTrans);
            } else {
                System.out.println("No transaction processed, no result to show");
                return;
            }
            double totTps = (double) totTrans * 1_000_000_000.0d / (double) elapsedNano;
            System.out.println("Transactions per second: " + BigDecimal.valueOf(totTps).setScale(3, RoundingMode.HALF_UP) + " (including connection and client overhead)");
            double rawTps = (double) totTrans * 1_000_000_000.0d / ((double) rawTime / (double) conf.getConcurrency());
            System.out.println("Transactions per second: " + BigDecimal.valueOf(rawTps).setScale(3, RoundingMode.HALF_UP) + " (excluding connection and client overhead)");
            double avgLatency = (double) rawTime / 1_000_000.0d / (double) totTrans;
            System.out.println("Average latency: " + BigDecimal.valueOf(avgLatency).setScale(3, RoundingMode.HALF_UP) + " ms");
            double stdDev = Math.sqrt((timings.stream().mapToDouble(x -> ((double) x) - avgLatency).map(x -> x * x).sum()) / ((double) totTrans)) / 1_000_000.0d;
            System.out.println("Latency stddev: " + BigDecimal.valueOf(stdDev).setScale(3, RoundingMode.HALF_UP) + " ms");
        } catch (InterruptedException | ExecutionException ex) {
            // should never happen
            System.out.println("Unexpected " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
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
