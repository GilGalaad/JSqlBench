package engine;

import static engine.BenchResult.ExecStatus.OK;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Callable;
import static main.JSqlBench.opts;

public class ProgressWorker implements Callable<BenchResult> {

    private final long deadline;
    private final ArrayList<Long> timings;
    private final Object lock;
    private static final int PERIOD_SEC = 60;

    public ProgressWorker(long deadline, ArrayList<Long> timings, Object lock) {
        this.deadline = deadline;
        this.timings = timings;
        this.lock = lock;
    }

    @Override
    public BenchResult call() throws Exception {
        BenchResult ret = new BenchResult();
        ret.setStatus(OK);
        // entering loop
        while (new Date().getTime() + (PERIOD_SEC * 1000) < deadline) {
            Thread.sleep(PERIOD_SEC * 1000);
            // calculating partial stats
            long totTrans = 0;
            long rawTime = 0;
            synchronized (lock) {
                totTrans += timings.size();
                for (Long t : timings) {
                    rawTime += t;
                }
            }
            double rawTps = (double) totTrans * (double) 1000000000 / ((double) rawTime / (double) opts.getConcurrency());
            double avgLatency = (double) rawTime / (double) 1000000 / (double) totTrans;
            System.out.println("Partial results: " + BigDecimal.valueOf(rawTps).setScale(3, RoundingMode.HALF_UP) + " tps, "
                    + BigDecimal.valueOf(avgLatency).setScale(3, RoundingMode.HALF_UP) + " ms latency");
        }
        return ret;
    }

}
