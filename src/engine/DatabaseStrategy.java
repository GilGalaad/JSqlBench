package engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import static main.JSqlBench.opts;

public abstract class DatabaseStrategy {

    public abstract Connection doConnect() throws SQLException;

    public abstract void dropTables(Connection c) throws SQLException;

    public abstract void createTables(Connection c) throws SQLException;

    public void populateTables(Connection c) throws SQLException {
        long startTime = System.nanoTime();
        System.out.print("Populating tables...");
        String sql = "insert into " + (opts.getSchema() != null ? opts.getSchema() + "." : "") + "bench_branches values (?,?)";
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            for (int i = 0; i < opts.getScale(); i++) {
                stmt.setLong(1, i + 1);
                stmt.setLong(2, 0);
                stmt.addBatch();
            }
            stmt.executeBatch();
            c.commit();
        }
        sql = "insert into " + (opts.getSchema() != null ? opts.getSchema() + "." : "") + "bench_tellers values (?,?,?)";
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            for (int i = 0; i < opts.getScale(); i++) {
                for (int j = 0; j < 10; j++) {
                    stmt.setLong(1, i * 10 + j + 1);
                    stmt.setLong(2, i + 1);
                    stmt.setLong(3, 0);
                    stmt.addBatch();
                }
            }
            stmt.executeBatch();
            c.commit();
        }
        sql = "insert into " + (opts.getSchema() != null ? opts.getSchema() + "." : "") + "bench_accounts values (?,?,?)";
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            for (int i = 0; i < opts.getScale(); i++) {
                for (int j = 0; j < 100000; j++) {
                    stmt.setLong(1, i * 100000 + j + 1);
                    stmt.setLong(2, i + 1);
                    stmt.setLong(3, 0);
                    stmt.addBatch();
                }
                System.out.print((i + 1) + "...");
                stmt.executeBatch();
                c.commit();
            }
        }
        long endTime = System.nanoTime();
        System.out.println("done! (" + String.format(Locale.ITALY, "%,d", (endTime - startTime) / 1_000_000L) + "ms)");
    }

    public void createIndexes(Connection c) throws SQLException {
        System.out.print("Creating primary keys...");
        String sql;
        String[] tables = new String[]{"bench_branches", "bench_tellers", "bench_accounts"};
        c.setAutoCommit(true);
        long startTime = System.nanoTime();
        for (String tb : tables) {
            sql = "create unique index pk_" + tb + " on " + (opts.getSchema() != null ? opts.getSchema() + "." + tb : tb)
                    + " (" + tb.substring(6, 7) + "id)" + (opts.getTablespace() != null ? " tablespace " + opts.getTablespace() : "");
            //System.out.println(sql);
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }
        long endTime = System.nanoTime();
        c.setAutoCommit(false);
        System.out.println("done! (" + String.format(Locale.ITALY, "%,d", (endTime - startTime) / 1_000_000L) + "ms)");
    }

    public abstract void analyzeTables(Connection c) throws SQLException;

    public abstract void runTransaction(Connection c, long bid, long tid, long aid, int delta) throws SQLException;

}
