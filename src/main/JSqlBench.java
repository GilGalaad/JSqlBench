package main;

import engine.BenchEngine;
import static main.BenchOpts.DbEngine.ORACLE;
import static main.BenchOpts.DbEngine.POSTGRES;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class JSqlBench {

    public static final BenchOpts opts = new BenchOpts();
    private static final Options cmdOpts = new Options();

    public static void main(String[] args) {
        // parsing command line arguments
        initCmdOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            line = parser.parse(cmdOpts, args);
        } catch (ParseException ex) {
            abort(ex.getMessage());
        }

        // residual parsing
        parseCommandLine(line);

        // run engine
        BenchEngine eng = new BenchEngine();
        eng.run();
    }

    private static void abort(String msg) {
        System.out.println(msg);
        printHelp();
        System.exit(1);
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("JSqlBench", cmdOpts);
    }

    private static void initCmdOptions() {
        // engine
        cmdOpts.addOption(Option.builder("e").longOpt("engine")
                .desc("Database engine\n"
                        + "Currently supported: \"oracle\" or \"postgres\"")
                .required(true).hasArg().argName("eng")
                .build());
        // hostname
        cmdOpts.addOption(Option.builder("h").longOpt("hostname")
                .desc("Database server's host name")
                .required(true).hasArg().argName("host")
                .build());
        // port
        cmdOpts.addOption(Option.builder("p").longOpt("port")
                .desc("Database server's port number")
                .required(true).hasArg().argName("port")
                .build());
        // database name
        cmdOpts.addOption(Option.builder("db").longOpt("database")
                .desc("Database or instance name (SID)")
                .required(true).hasArg().argName("db")
                .build());
        // username
        cmdOpts.addOption(Option.builder("U").longOpt("username")
                .required(true).hasArg().argName("user")
                .build());
        // password
        cmdOpts.addOption(Option.builder("P").longOpt("password")
                .hasArg().argName("pwd")
                .build());
        // schema
        cmdOpts.addOption(Option.builder("S").longOpt("schema")
                .desc("Create tables in the specified namespace or schema, rather than the default one")
                .hasArg().argName("schema")
                .build());
        // tablespace
        cmdOpts.addOption(Option.builder("T").longOpt("tablespace")
                .desc("Create tables in the specified tablespace, rather than the default tablespace")
                .hasArg().argName("tbsp")
                .build());
        // nologging
        cmdOpts.addOption(Option.builder("nl").longOpt("nologging")
                .desc("Create tables in nologging mode")
                .build());
        // scale
        cmdOpts.addOption(Option.builder("s").longOpt("scale")
                .desc("Initialization scale factor, 1 = 100.000 rows\n"
                        + "The initialization scale factor (-s) should be at least as large as the largest number of clients you intend to test (-c), "
                        + "else you'll mostly be measuring update contention\n"
                        + "[default 1]")
                .hasArg().argName("scale")
                .build());
        // concurrency
        cmdOpts.addOption(Option.builder("c").longOpt("concurrency")
                .desc("Number of concurrent clients simulated (Degree Of Parallelism) [default 1]")
                .hasArg().argName("dop")
                .build());
        // time
        cmdOpts.addOption(Option.builder("t").longOpt("time")
                .desc("Run the test for this many seconds")
                .required(true).hasArg().argName("seconds")
                .build());
    }

    private static void parseCommandLine(CommandLine line) {
        if (line.getOptionValue("e").equalsIgnoreCase("oracle")) {
            opts.setEngine(ORACLE);
        } else if (line.getOptionValue("e").equalsIgnoreCase("postgres")) {
            opts.setEngine(POSTGRES);
        } else {
            abort("Unsupported database engine: " + line.getOptionValue("e"));
        }
        opts.setHostname(line.getOptionValue("h"));
        try {
            int port = Integer.valueOf(line.getOptionValue("p"));
            if (port <= 0) {
                throw new NumberFormatException();
            }
            opts.setPort(port);
        } catch (NumberFormatException ex) {
            abort("Illegal port number: " + line.getOptionValue("p"));
        }
        opts.setDbname(line.getOptionValue("db"));
        opts.setUsername(line.getOptionValue("U"));
        if (line.hasOption("P")) {
            opts.setPassword(line.getOptionValue("P"));
        }
        if (line.hasOption("S")) {
            opts.setSchema(line.getOptionValue("S"));
        }
        if (line.hasOption("T")) {
            opts.setTablespace(line.getOptionValue("T"));
        }
        if (line.hasOption("nl")) {
            opts.setNologging(true);
        }
        if (line.hasOption("s")) {
            try {
                int scale = Integer.valueOf(line.getOptionValue("s"));
                if (scale <= 0) {
                    throw new NumberFormatException();
                }
                opts.setScale(scale);
            } catch (NumberFormatException ex) {
                abort("Illegal scale factor: " + line.getOptionValue("s"));
            }
        }
        if (line.hasOption("s")) {
            try {
                int scale = Integer.valueOf(line.getOptionValue("s"));
                if (scale <= 0) {
                    throw new NumberFormatException();
                }
                opts.setScale(scale);
            } catch (NumberFormatException ex) {
                abort("Illegal scale factor: " + line.getOptionValue("s"));
            }
        }
        if (line.hasOption("c")) {
            try {
                int concurrency = Integer.valueOf(line.getOptionValue("c"));
                if (concurrency <= 0) {
                    throw new NumberFormatException();
                }
                opts.setConcurrency(concurrency);
            } catch (NumberFormatException ex) {
                abort("Illegal concurrency: " + line.getOptionValue("c"));
            }
        }
        try {
            int time = Integer.valueOf(line.getOptionValue("t"));
            if (time <= 0) {
                throw new NumberFormatException();
            }
            opts.setTime(time);
        } catch (NumberFormatException ex) {
            abort("Illegal number of seconds: " + line.getOptionValue("t"));
        }
        //System.out.println(opts.toString());
    }

}
