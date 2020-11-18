package main;

import picocli.CommandLine;

public class Launcher {

    public static void main(String[] args) {
        CommandLine cl = new CommandLine(new JSqlBenchCommand());
        cl.setCaseInsensitiveEnumValuesAllowed(true);
        cl.setUsageHelpWidth(150);
        cl.setUsageHelpLongOptionsMaxWidth(50);
        int rc = cl.execute(args);
        System.exit(rc);
    }

}
