package engine;

import static engine.BenchResult.ExecStatus.KO;
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
import static main.BenchOpts.DbEngine.ORACLE;
import static main.BenchOpts.DbEngine.POSTGRES;
import static main.JSqlBench.opts;

public class BenchEngine {

    private final DatabaseStrategy str;
    private final ArrayList<Long> timings = new ArrayList<>();
    private final Object lock = new Object();

    public BenchEngine() {
        try {
            if (opts.getEngine() == ORACLE) {
                str = new OracleStrategy();
            } else if (opts.getEngine() == POSTGRES) {
                str = new PgStrategy();
            } else {
                throw new UnsupportedOperationException("Unsupported database engine:" + opts.getEngine());
            }
        } catch (ClassNotFoundException ex) {
            throw new UnsupportedOperationException(ex.getMessage(), ex);
        }
    }

    public void run() {
        System.out.println("*** PREPARING FOR BENCHMARK ***");
        try {
            prepareDatabase();
        } catch (SQLException ex) {
            System.out.println("Error while preparing database for benchmark: " + ex.getMessage());
            System.exit(1);
        }

        try {
            System.out.println("Starting " + opts.getConcurrency() + " concurrent threads...");
            // let settle down a bit
            Thread.sleep(5000);
            // preparing threads
            ExecutorService tPool = Executors.newFixedThreadPool(opts.getConcurrency() + 1);
            ArrayList<Callable<BenchResult>> tList = new ArrayList<>(opts.getConcurrency() + 1);
            List<Future<BenchResult>> tRes = null;
            // calculate execution deadline
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.SECOND, opts.getTime());
            long deadline = cal.getTimeInMillis();
            for (int i = 0; i < opts.getConcurrency(); i++) {
                tList.add(new DatabaseWorker(str, deadline, timings, lock));
            }
            tList.add(new ProgressWorker(deadline, timings, lock));
            // launching threads
            long startTime = System.nanoTime();
            tRes = tPool.invokeAll(tList);
            tPool.shutdown();
            waitAll(tRes);
            long endTime = System.nanoTime();

            // calculating metrics
            System.out.println("*** BENCHMARK RESULTS ***");
            System.out.println("Scale factor: " + opts.getScale());
            System.out.println("Number of concurrent clients: " + opts.getConcurrency());
            long totTime = endTime - startTime;
            System.out.println("Total time elapsed: " + (totTime / 1000000000L) + " seconds");
            for (Future<BenchResult> f : tRes) {
                if (f.get().getStatus() == KO) {
                    System.out.println("Thead reported exception: " + f.get().getEx().getMessage());
                }
            }
            long totTrans = 0;
            long rawTime = 0;
            totTrans += timings.size();
            for (Long t : timings) {
                rawTime += t;
            }
            if (totTrans != 0) {
                System.out.println("Total number of transactions processed: " + totTrans);
            } else {
                System.out.println("No transaction processed, no result to show");
                System.exit(1);
            }
            double totTps = (double) totTrans * (double) 1000000000 / (double) totTime;
            System.out.println("Transactions per second: " + BigDecimal.valueOf(totTps).setScale(3, RoundingMode.HALF_UP) + " (including connection and client overhead)");
            double rawTps = (double) totTrans * (double) 1000000000 / ((double) rawTime / (double) opts.getConcurrency());
            System.out.println("Transactions per second: " + BigDecimal.valueOf(rawTps).setScale(3, RoundingMode.HALF_UP) + " (excluding connection and client overhead)");
            double avgLatency = (double) rawTime / (double) 1000000 / (double) totTrans;
            System.out.println("Average latency: " + BigDecimal.valueOf(avgLatency).setScale(3, RoundingMode.HALF_UP) + " ms");
        } catch (InterruptedException | ExecutionException ex) {
            // should never happen
            System.out.println(ex.getMessage());
        }
    }

    private void waitAll(List<Future<BenchResult>> tRes) {
        try {
            for (Future f : tRes) {
                f.get();
            }
        } catch (InterruptedException | ExecutionException ex) {
            // do nothing, will be handled later
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

}
