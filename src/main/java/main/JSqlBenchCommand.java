package main;

import engine.BenchEngine;
import engine.dto.BenchConf;
import engine.dto.DbEngine;
import lombok.extern.log4j.Log4j2;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

@Log4j2
@Command(name = "JSqlBench", sortOptions = false, abbreviateSynopsis = true)
public class JSqlBenchCommand implements Callable<Integer> {

    @Option(names = "--engine", required = true, description = "Database engine. Currently supported: Oracle and Postgres")
    private DbEngine engine;

    @Option(names = "--host", required = false, defaultValue = "localhost", description = "Database server's hostname (default: ${DEFAULT-VALUE})")
    private String host;

    @Option(names = "--port", required = false, converter = PortConverter.class, description = "Database server's port (default: 1521 for Oracle and 5432 for Postgres)")
    private Integer port;

    @Option(names = "--dbname", required = true, description = "Database or instance name (SID)")
    private String dbname;

    @Option(names = "--username", required = true, description = "Username used to log in")
    private String username;

    @Option(names = "--password", required = false, description = "Password used to log in")
    private String password;

    @Option(names = "--schema", required = false, converter = SqlIdentifierConverter.class, description = "Create objects in the specified namespace or schema, rather than the default one")
    private String schema;

    @Option(names = "--tablespace", required = false, converter = SqlIdentifierConverter.class, description = "Create objects in the specified tablespace, rather than the default one")
    private String tablespace;

    @Option(names = "--nologging", required = false, defaultValue = "false", description = "Create tables in nologging mode")
    private boolean nologging;

    @Option(names = "--scale", required = false, defaultValue = "1", converter = PositiveIntegerConverter.class,
            description = "Initialization scale factor, 1 = 100.000 rows. "
                    + "The initialization scale factor should be at least as large as the largest number of clients you intend to test, "
                    + "else you'll mostly be measuring update contention (default: ${DEFAULT-VALUE})")
    private int scale;

    @Option(names = "--concurrency", required = false, defaultValue = "1", converter = PositiveIntegerConverter.class, description = "Number of concurrent clients simulated (default: ${DEFAULT-VALUE})")
    private int concurrency;

    @Option(names = "--time", required = false, defaultValue = "300", converter = PositiveIntegerConverter.class,
            description = "Run the test for this many seconds. Never believe any test that runs for only a few seconds, "
                    + "it is a good practice to make the run last at least a few minutes. "
                    + "In some cases you could need hours to get numbers that are reproducible (default: ${DEFAULT-VALUE})")
    private int time;

    @Option(names = "--read-only", required = false, defaultValue = "false", description = "Simulate a read only worlkoad")
    private boolean readOnly;

    @Option(names = "--help", usageHelp = true, description = "Print this help and exit")
    private boolean help;

    public static class PositiveIntegerConverter implements ITypeConverter<Integer> {

        @Override
        public Integer convert(String value) {
            int parsed = parseInteger(value, "a positive integer");
            if (parsed <= 0) {
                throw new TypeConversionException("expected a positive integer, got '" + value + "'");
            }
            return parsed;
        }

    }

    public static class PortConverter implements ITypeConverter<Integer> {

        @Override
        public Integer convert(String value) {
            int parsed = parseInteger(value, "an integer between 1 and 65535");
            if (parsed < 1 || parsed > 65535) {
                throw new TypeConversionException("expected an integer between 1 and 65535, got '" + value + "'");
            }
            return parsed;
        }

    }

    public static class SqlIdentifierConverter implements ITypeConverter<String> {

        private static final Pattern PORTABLE_SQL_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");

        @Override
        public String convert(String value) {
            if (!PORTABLE_SQL_IDENTIFIER.matcher(value).matches()) {
                throw new TypeConversionException("expected a portable unquoted SQL identifier, got '" + value + "'");
            }
            return value;
        }

    }

    private static int parseInteger(String value, String expected) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            throw new TypeConversionException("expected " + expected + ", got '" + value + "'");
        }
    }

    @Override
    public Integer call() throws Exception {
        int selectedPort = Objects.requireNonNullElseGet(port, () -> switch (engine) {
            case ORACLE -> 1521;
            case POSTGRES -> 5432;
        });
        BenchConf conf = new BenchConf(engine, host, selectedPort, dbname, username, password, schema, tablespace, nologging, scale, concurrency, time, readOnly);

        try {
            BenchEngine eng = new BenchEngine(conf);
            eng.run();
        } catch (Exception ex) {
            log.error(ex.getMessage());
            return 1;
        }

        return 0;
    }

}
