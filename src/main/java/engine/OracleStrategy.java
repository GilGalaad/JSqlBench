package engine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import static main.JSqlBench.opts;

public class OracleStrategy extends DatabaseStrategy {

    public OracleStrategy() throws ClassNotFoundException {
        Class.forName("oracle.jdbc.OracleDriver");
    }

    @Override
    public Connection doConnect() throws SQLException {
        String connString = "jdbc:oracle:thin:@//%s:%d/%s";
        connString = String.format(connString, opts.getHostname(), opts.getPort(), opts.getDbname());
        Connection c = DriverManager.getConnection(connString, opts.getUsername(), opts.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    @Override
    public void dropTables(Connection c) throws SQLException {
        System.out.print("Dropping tables...");
        String sql;
        String[] tables = new String[]{"bench_branches", "bench_tellers", "bench_accounts", "bench_history"};
        long startTime = System.nanoTime();
        for (String tb : tables) {
            sql = "drop table " + (opts.getSchema() != null ? opts.getSchema() + "." + tb : tb) + " cascade constraints purge";
            //System.out.println(sql);
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            } catch (SQLException ex) {
                // ignoring error ORA-00942: table or view does not exist
                if (ex.getErrorCode() != 942) {
                    throw ex;
                }
            }
        }
        long endTime = System.nanoTime();
        System.out.println("done! (" + String.format(Locale.ITALY, "%,d", (endTime - startTime) / 1000000L) + "ms)");
    }

    @Override
    public void createTables(Connection c) throws SQLException {
        System.out.print("Creating tables...");
        String sql;
        String[] tableDefs = new String[]{"bench_branches (bid number(38,0) not null, bbalance number(38,0))",
            "bench_tellers (tid number(38,0) not null, bid number(38,0), tbalance number(38,0))",
            "bench_accounts (aid number(38,0) not null, bid number(38,0), abalance number(38,0))",
            "bench_history (tid number(38,0), bid number(38,0), aid number(38,0), delta number(38,0), mtime timestamp(6))"};
        long startTime = System.nanoTime();
        for (String tbd : tableDefs) {
            sql = "create table " + (opts.getSchema() != null ? opts.getSchema() + "." : "")
                    + tbd
                    + (opts.isNologging() ? " nologging" : "")
                    + (opts.getTablespace() != null ? " tablespace " + opts.getTablespace() : "");
            //System.out.println(sql);
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }
        long endTime = System.nanoTime();
        System.out.println("done! (" + String.format(Locale.ITALY, "%,d", (endTime - startTime) / 1000000L) + "ms)");
    }

    @Override
    public void analyzeTables(Connection c) throws SQLException {
        System.out.print("Analyzing...");
        String sql;
        String[] tables = new String[]{"bench_branches", "bench_tellers", "bench_accounts", "bench_history"};
        long startTime = System.nanoTime();
        for (String tb : tables) {
            sql = "begin dbms_stats.gather_table_stats(ownname => "
                    + (opts.getSchema() != null ? "'" + opts.getSchema().toUpperCase() + "', " : "user, ")
                    + "tabname => '" + tb.toUpperCase() + "', "
                    + "estimate_percent => dbms_stats.auto_sample_size, degree=> dbms_stats.auto_degree, granularity => 'ALL'); end;";
            //System.out.println(sql);
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }
        long endTime = System.nanoTime();
        System.out.println("done! (" + String.format(Locale.ITALY, "%,d", (endTime - startTime) / 1000000L) + "ms)");
    }

    @Override
    public void runTransaction(Connection c, long bid, long tid, long aid, int delta) throws SQLException {
        String sql = "update " + (opts.getSchema() != null ? opts.getSchema() + "." : "") + "bench_accounts set abalance = abalance + ? where aid = ?";
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            stmt.setInt(1, delta);
            stmt.setLong(2, aid);
            stmt.executeUpdate();
        }
        sql = "select abalance from " + (opts.getSchema() != null ? opts.getSchema() + "." : "") + "bench_accounts where aid = ?";
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            stmt.setLong(1, aid);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    rs.getInt(1);
                }
            }
        }
        sql = "update " + (opts.getSchema() != null ? opts.getSchema() + "." : "") + "bench_tellers set tbalance = tbalance + ? where tid = ?";
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            stmt.setInt(1, delta);
            stmt.setLong(2, tid);
            stmt.executeUpdate();
        }
        sql = "update " + (opts.getSchema() != null ? opts.getSchema() + "." : "") + "bench_branches set bbalance = bbalance + ? where bid = ?";
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            stmt.setInt(1, delta);
            stmt.setLong(2, bid);
            stmt.executeUpdate();
        }
        sql = "insert into " + (opts.getSchema() != null ? opts.getSchema() + "." : "") + "bench_history (tid, bid, aid, delta, mtime) values (?, ?, ?, ?, SYSTIMESTAMP)";
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            stmt.setLong(1, tid);
            stmt.setLong(2, bid);
            stmt.setLong(3, bid);
            stmt.setInt(4, delta);
            stmt.executeUpdate();
        }
        c.commit();
    }

}
