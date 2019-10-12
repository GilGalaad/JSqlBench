package main;

import engine.BenchEngine;
import engine.dto.BenchConf;
import engine.dto.BenchConf.DbEngine;
import static engine.dto.BenchConf.DbEngine.ORACLE;
import static engine.dto.BenchConf.DbEngine.POSTGRES;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "JSqlBench",
        sortOptions = false, abbreviateSynopsis = true,
        usageHelpAutoWidth = true)
public class JSqlBenchCommand implements Callable<Integer> {

    @Option(names = "--engine", required = true, description = "Database engine. Currently supported: ${COMPLETION-CANDIDATES}")
    private DbEngine engine;

    @Option(names = "--host", required = false, defaultValue = "localhost", description = "Database server's hostname (default: ${DEFAULT-VALUE})")
    private String host;

    @Option(names = "--port", required = false, description = "Database server's port (default: 1521 for ORACLE and 5432 for POSTGRES)")
    private Integer port;

    @Option(names = "--dbname", required = true, description = "Database or instance name (SID)")
    private String dbname;

    @Option(names = "--username", required = true, description = "Username used to log in")
    private String username;

    @Option(names = "--password", required = false, description = "Password used to log in")
    private String password;

    @Option(names = "--schema", required = false, description = "Create objects in the specified namespace or schema, rather than the default one")
    private String schema;

    @Option(names = "--tablespace", required = false, description = "Create objects in the specified tablespace, rather than the default one")
    private String tablespace;

    @Option(names = "--nologging", required = false, description = "Create tables in nologging mode")
    private boolean nologging;

    @Option(names = "--scale", required = false, defaultValue = "1",
            description = "Initialization scale factor, 1 = 100.000 rows. "
            + "The initialization scale factor should be at least as large as the largest number of clients you intend to test, "
            + "else you'll mostly be measuring update contention (default: ${DEFAULT-VALUE})")
    private int scale;

    @Option(names = "--concurrency", required = false, defaultValue = "1", description = "Number of concurrent clients simulated (default: ${DEFAULT-VALUE})")
    private Integer concurrency;

    @Option(names = "--time", required = false, defaultValue = "300",
            description = "Run the test for this many seconds. Never believe any test that runs for only a few seconds, "
            + "it is a good practice to make the run last at least a few minutes. "
            + "In some cases you could need hours to get numbers that are reproducible (default: ${DEFAULT-VALUE})")
    private Integer time;

    @Option(names = "--help", usageHelp = true, description = "Print this help and exit")
    private boolean help;

    @Override
    public Integer call() throws Exception {
        BenchConf conf = new BenchConf();
        conf.setEngine(engine);
        conf.setHost(host);
        if (port != null) {
            conf.setPort(port);
        } else {
            if (engine == ORACLE) {
                conf.setPort(1521);
            } else if (engine == POSTGRES) {
                conf.setPort(5432);
            } else {
                throw new AssertionError("Unreachable code branch");
            }
        }
        conf.setDbname(dbname);
        conf.setUsername(username);
        conf.setPassword(password);
        conf.setSchema(schema);
        conf.setTablespace(tablespace);
        conf.setNologging(nologging);
        conf.setScale(scale);
        conf.setConcurrency(concurrency);
        conf.setTime(time);

        try {
            BenchEngine eng = new BenchEngine(conf);
            eng.run();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return 1;
        }

        return 0;
    }

}
