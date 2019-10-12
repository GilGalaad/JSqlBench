package engine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import static main.JSqlBench.opts;

public class PgStrategy extends DatabaseStrategy {

    public PgStrategy() throws ClassNotFoundException {
        Logger logger = Logger.getLogger("org.postgresql");
        logger.setLevel(Level.OFF);
        Class.forName("org.postgresql.Driver");
    }

    @Override
    public Connection doConnect() throws SQLException {
        String connString = "jdbc:postgresql://%s:%d/%s";
        connString = String.format(connString, opts.getHostname(), opts.getPort(), opts.getDbname());
        Properties props = new Properties();
        props.setProperty("user", opts.getUsername());
        if (opts.getPassword() != null) {
            props.setProperty("password", opts.getPassword());
        }
        Connection c = DriverManager.getConnection(connString, props);
        c.setAutoCommit(false);
        return c;
    }

    @Override
    public void dropTables(Connection c) throws SQLException {
        System.out.print("Dropping tables...");
        String sql;
        String[] tables = new String[]{"bench_branches", "bench_tellers", "bench_accounts", "bench_history"};
        c.setAutoCommit(true);
        long startTime = System.nanoTime();
        for (String tb : tables) {
            sql = "drop table if exists " + (opts.getSchema() != null ? opts.getSchema() + "." + tb : tb) + " cascade";
            //System.out.println(sql);
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }
        long endTime = System.nanoTime();
        c.setAutoCommit(false);
        System.out.println("done! (" + String.format(Locale.ITALY, "%,d", (endTime - startTime) / 1000000L) + "ms)");
    }

    @Override
    public void createTables(Connection c) throws SQLException {
        System.out.print("Creating tables...");
        String sql;
        String[] tableDefs = new String[]{"bench_branches (bid bigint not null, bbalance bigint)",
            "bench_tellers (tid bigint not null, bid bigint, tbalance bigint)",
            "bench_accounts (aid bigint not null, bid bigint, abalance bigint)",
            "bench_history (tid bigint, bid bigint, aid bigint, delta bigint, mtime timestamp(6))"};
        c.setAutoCommit(true);
        long startTime = System.nanoTime();
        for (String tbd : tableDefs) {
            sql = "create" + (opts.isNologging() ? " unlogged" : "") + " table " + (opts.getSchema() != null ? opts.getSchema() + "." : "")
                    + tbd
                    + (opts.getTablespace() != null ? " tablespace " + opts.getTablespace() : "");
            //System.out.println(sql);
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }
        long endTime = System.nanoTime();
        c.setAutoCommit(false);
        System.out.println("done! (" + String.format(Locale.ITALY, "%,d", (endTime - startTime) / 1000000L) + "ms)");
    }

    @Override
    public void analyzeTables(Connection c) throws SQLException {
        System.out.print("Vacuuming...");
        String sql;
        String[] tables = new String[]{"bench_branches", "bench_tellers", "bench_accounts", "bench_history"};
        c.setAutoCommit(true);
        long startTime = System.nanoTime();
        for (String tb : tables) {
            sql = "vacuum analyze " + tb;
            //System.out.println(sql);
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }
        long endTime = System.nanoTime();
        c.setAutoCommit(false);
        System.out.println("done! (" + String.format(Locale.ITALY, "%,d", (endTime - startTime) / 1000000L) + "ms)");
    }

    @Override
    public void runTransaction(Connection c, long bid, long tid, long aid, int delta) throws SQLException {
        try (PreparedStatement stmt = c.prepareStatement("update bench_accounts set abalance = abalance + ? where aid = ?")) {
            stmt.setInt(1, delta);
            stmt.setLong(2, aid);
            stmt.executeUpdate();
        }
        try (PreparedStatement stmt = c.prepareStatement("select abalance from bench_accounts where aid = ?")) {
            stmt.setLong(1, aid);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    rs.getInt(1);
                }
            }
        }
        try (PreparedStatement stmt = c.prepareStatement("update bench_tellers set tbalance = tbalance + ? where tid = ?")) {
            stmt.setInt(1, delta);
            stmt.setLong(2, tid);
            stmt.executeUpdate();
        }
        try (PreparedStatement stmt = c.prepareStatement("update bench_branches set bbalance = bbalance + ? where bid = ?")) {
            stmt.setInt(1, delta);
            stmt.setLong(2, bid);
            stmt.executeUpdate();
        }
        try (PreparedStatement stmt = c.prepareStatement("insert into bench_history (tid, bid, aid, delta, mtime) values (?, ?, ?, ?, CURRENT_TIMESTAMP)")) {
            stmt.setLong(1, tid);
            stmt.setLong(2, bid);
            stmt.setLong(3, bid);
            stmt.setInt(4, delta);
            stmt.executeUpdate();
        }
        c.commit();
    }

}
