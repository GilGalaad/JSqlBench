package engine;

import engine.dto.BenchConf;
import engine.dto.WorkerContext;
import engine.dto.WorkerResult;
import engine.strategy.DatabaseStrategy;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import static engine.dto.WorkerStatus.KO;
import static engine.dto.WorkerStatus.OK;

@AllArgsConstructor
@Log4j2
public class DatabaseWorker implements Callable<WorkerResult> {

    private final BenchConf conf;
    private final DatabaseStrategy str;
    private final long deadline;
    private final WorkerContext context;

    @Override
    public WorkerResult call() throws Exception {
        // connecting to database
        try (Connection c = str.doConnect()) {
            // entering loop
            while (true) {
                // randomizing ids
                long bid = ThreadLocalRandom.current().nextLong(1, conf.scale() + 1);
                long tid = ThreadLocalRandom.current().nextLong(1, conf.scale() * 10L + 1);
                long aid = ThreadLocalRandom.current().nextLong(1, conf.scale() * 100000L + 1);
                int delta = ThreadLocalRandom.current().nextInt(-5000, 5001);
                long startTime = System.nanoTime();
                if (startTime - deadline >= 0) {
                    break;
                }
                if (conf.readOnly()) {
                    str.runReadOnlyTransaction(c, bid, tid, aid);
                } else {
                    str.runWriteTransaction(c, bid, tid, aid, delta);
                }
                long endTime = System.nanoTime();
                context.addSample(endTime - startTime);
            }
        } catch (SQLException | RuntimeException ex) {
            // if something goes wrong, return anyway what done until now
            return new WorkerResult(KO, ex);
        }
        return new WorkerResult(OK, null);
    }

}
