package engine;

import static engine.BenchResult.ExecStatus.KO;
import static engine.BenchResult.ExecStatus.OK;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import static main.JSqlBench.opts;

public class DatabaseWorker implements Callable<BenchResult> {

    private final DatabaseStrategy str;
    private final long deadline;
    private final ArrayList<Long> timings;
    private final Object lock;

    public DatabaseWorker(DatabaseStrategy str, long deadline, ArrayList<Long> timings, Object lock) {
        this.str = str;
        this.deadline = deadline;
        this.timings = timings;
        this.lock = lock;
    }

    @Override
    public BenchResult call() throws Exception {
        BenchResult ret = new BenchResult();
        // connecting to database
        try (Connection c = str.doConnect()) {
            // entering loop
            while (new Date().getTime() < deadline) {
                // randomizing ids
                long bid = ThreadLocalRandom.current().nextLong(1, opts.getScale() + 1);
                long tid = ThreadLocalRandom.current().nextLong(1, opts.getScale() * 10 + 1);
                long aid = ThreadLocalRandom.current().nextLong(1, opts.getScale() * 100000 + 1);
                int delta = ThreadLocalRandom.current().nextInt(-5000, 5001);
                long startTime = System.nanoTime();
                str.runTransaction(c, bid, tid, aid, delta);
                long endTime = System.nanoTime();
                synchronized (lock) {
                    timings.add(endTime - startTime);
                }
            }
        } catch (SQLException | RuntimeException ex) {
            // if something goes wrong, return anyway what done until now
            ret.setStatus(KO);
            ret.setEx(ex);
            return ret;
        }
        ret.setStatus(OK);
        return ret;
    }

}
