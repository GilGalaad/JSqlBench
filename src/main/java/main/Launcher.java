package main;

import picocli.CommandLine;

public class Launcher {

    public static void main(String[] args) {
        CommandLine cl = new CommandLine(new JSqlBenchCommand());
        cl.setCaseInsensitiveEnumValuesAllowed(true);
        int rc = cl.execute(args);
        System.exit(rc);
    }

}
