package engine;

import static engine.BenchResult.ExecStatus.OK;
import engine.dto.BenchConf;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Callable;

public class ProgressWorker implements Callable<BenchResult> {

    private static final int INTERVAL_SEC = 60;

    private final BenchConf conf;
    private final long deadline;
    private final ArrayList<Long> timings;
    private final Object lock;

    public ProgressWorker(BenchConf conf, long deadline, ArrayList<Long> timings, Object lock) {
        this.conf = conf;
        this.deadline = deadline;
        this.timings = timings;
        this.lock = lock;
    }

    @Override
    public BenchResult call() throws Exception {
        BenchResult ret = new BenchResult();
        ret.setStatus(OK);
        // entering loop
        while (new Date().getTime() + (INTERVAL_SEC * 1000) < deadline) {
            Thread.sleep(INTERVAL_SEC * 1000);
            // calculating partial stats
            long totTrans, rawTime;
            double rawTps, avgLatency, stdDev;
            synchronized (lock) {
                totTrans = timings.size();
                rawTime = timings.stream().mapToLong(i -> i).sum();
                rawTps = (double) totTrans * 1_000_000_000.0d / ((double) rawTime / (double) conf.getConcurrency());
                avgLatency = (double) rawTime / 1_000_000.0d / (double) totTrans;
                stdDev = Math.sqrt((timings.stream().mapToDouble(i -> ((double) i) - avgLatency).map(i -> i * i).sum()) / ((double) totTrans)) / 1_000_000.0d;
            }
            System.out.println("Partial results: "
                    + BigDecimal.valueOf(rawTps).setScale(3, RoundingMode.HALF_UP) + " tps, "
                    + BigDecimal.valueOf(avgLatency).setScale(3, RoundingMode.HALF_UP) + " ms latency, "
                    + BigDecimal.valueOf(stdDev).setScale(3, RoundingMode.HALF_UP) + " stddev");
        }
        return ret;
    }

}
