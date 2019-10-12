package engine;

import engine.dto.BenchConf;
import engine.dto.BenchResult;
import static engine.dto.BenchResult.ExecStatus.OK;
import engine.utils.MetricProvider;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Callable;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ProgressWorker implements Callable<BenchResult> {

    private static final int INTERVAL_SEC = 60;

    private final BenchConf conf;
    private final long deadline;
    private final ArrayList<Double> timings;
    private final Object lock;

    public ProgressWorker(BenchConf conf, long deadline, ArrayList<Double> timings, Object lock) {
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
            MetricProvider mp;
            synchronized (lock) {
                mp = new MetricProvider(timings);
            }
            int totTrans = mp.getCount();
            double rawTime = mp.getSum();
            double rawTps = (double) totTrans / (rawTime / 1_000d / (double) conf.getConcurrency());
            double avgLatency = mp.getMean();
            double stdDev = totTrans > 1 ? mp.getStddev() : 0d;
            log.info("Partial results: {} tps, {} ms latency, {} stddev",
                    BigDecimal.valueOf(rawTps).setScale(3, RoundingMode.HALF_UP),
                    BigDecimal.valueOf(avgLatency).setScale(3, RoundingMode.HALF_UP),
                    BigDecimal.valueOf(stdDev).setScale(3, RoundingMode.HALF_UP));
        }
        return ret;
    }

}
