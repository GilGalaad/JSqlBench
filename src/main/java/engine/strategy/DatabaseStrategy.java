package engine.strategy;

import static engine.CommonUtils.smartElapsed;
import engine.dto.BenchConf;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import lombok.extern.log4j.Log4j2;

@Log4j2
public abstract class DatabaseStrategy {

    protected static final List<String> tables = Arrays.asList(new String[]{"bench_branches", "bench_tellers", "bench_accounts", "bench_history"});

    protected static final String INSERT_BRANCHES_STMT = "INSERT INTO %sbench_branches VALUES (?, ?)";
    protected static final String INSERT_TELLERS_STMT = "INSERT INTO %sbench_tellers VALUES (?, ?, ?)";
    protected static final String INSERT_ACCOUNTS_STMT = "INSERT INTO %sbench_accounts VALUES (?, ?, ?)";

    protected static final String IDX_BRANCHES_STMT = "CREATE UNIQUE INDEX bench_branches_pk ON %sbench_branches (bid) %s";
    protected static final String IDX_TELLERS_STMT = "CREATE UNIQUE INDEX bench_tellers_pk ON %sbench_tellers (tid) %s";
    protected static final String IDX_ACCOUNTS_STMT = "CREATE UNIQUE INDEX bench_accounts_pk ON %sbench_accounts (aid) %s";

    protected static final String UPDATE_ACCOUNTS_STMT = "UPDATE %sbench_accounts SET abalance = abalance + ? WHERE aid = ?";
    protected static final String SELECT_ACCOUNTS_STMT = "SELECT abalance FROM %sbench_accounts WHERE aid = ?";
    protected static final String UPDATE_TELLERS_STMT = "UPDATE %sbench_tellers SET tbalance = tbalance + ? WHERE tid = ?";
    protected static final String UPDATE_BRANCHES_STMT = "UPDATE %sbench_branches SET bbalance = bbalance + ? WHERE bid = ?";
    protected static final String INSERT_HISTORY_STMT = "INSERT INTO %sbench_history (tid, bid, aid, delta, mtime) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)";

    protected final BenchConf conf;

    public DatabaseStrategy(BenchConf conf) throws ClassNotFoundException {
        this.conf = conf;
    }

    public abstract Connection doConnect() throws SQLException;

    public abstract void dropTables(Connection c) throws SQLException;

    public abstract void createTables(Connection c) throws SQLException;

    public void populateTables(Connection c) throws SQLException {
        log.info("Populating tables...");
        long startTime = System.nanoTime();
        String sql = String.format(INSERT_BRANCHES_STMT, getSchemaPrefix());
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            for (int i = 0; i < conf.getScale(); i++) {
                stmt.setLong(1, i + 1);
                stmt.setLong(2, 0);
                stmt.addBatch();
            }
            stmt.executeBatch();
            c.commit();
        }
        sql = String.format(INSERT_TELLERS_STMT, getSchemaPrefix());
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            for (int i = 0; i < conf.getScale(); i++) {
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
        sql = String.format(INSERT_ACCOUNTS_STMT, getSchemaPrefix());
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            for (int i = 0; i < conf.getScale(); i++) {
                for (int j = 0; j < 100000; j++) {
                    stmt.setLong(1, i * 100000 + j + 1);
                    stmt.setLong(2, i + 1);
                    stmt.setLong(3, 0);
                    stmt.addBatch();
                }
                log.info("{}...", i + 1);
                stmt.executeBatch();
                c.commit();
            }
        }
        long endTime = System.nanoTime();
        log.info("done! ({})\n", smartElapsed(endTime - startTime));
    }

    public void createIndexes(Connection c) throws SQLException {
        log.info("Creating indexes...");
        c.setAutoCommit(true);
        long startTime = System.nanoTime();
        String sql = String.format(IDX_BRANCHES_STMT, getSchemaPrefix(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        sql = String.format(IDX_TELLERS_STMT, getSchemaPrefix(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        sql = String.format(IDX_ACCOUNTS_STMT, getSchemaPrefix(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        long endTime = System.nanoTime();
        c.setAutoCommit(false);
        log.info("done! ({})\n", smartElapsed(endTime - startTime));
    }

    public abstract void analyzeTables(Connection c) throws SQLException;

    public void runWriteTransaction(Connection c, long bid, long tid, long aid, int delta) throws SQLException {
        try (PreparedStatement stmt = c.prepareStatement(String.format(UPDATE_ACCOUNTS_STMT, getSchemaPrefix()))) {
            stmt.setInt(1, delta);
            stmt.setLong(2, aid);
            stmt.executeUpdate();
        }
        try (PreparedStatement stmt = c.prepareStatement(String.format(SELECT_ACCOUNTS_STMT, getSchemaPrefix()))) {
            stmt.setLong(1, aid);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    rs.getInt(1);
                }
            }
        }
        try (PreparedStatement stmt = c.prepareStatement(String.format(UPDATE_TELLERS_STMT, getSchemaPrefix()))) {
            stmt.setInt(1, delta);
            stmt.setLong(2, tid);
            stmt.executeUpdate();
        }
        try (PreparedStatement stmt = c.prepareStatement(String.format(UPDATE_BRANCHES_STMT, getSchemaPrefix()))) {
            stmt.setInt(1, delta);
            stmt.setLong(2, bid);
            stmt.executeUpdate();
        }
        try (PreparedStatement stmt = c.prepareStatement(String.format(INSERT_HISTORY_STMT, getSchemaPrefix()))) {
            stmt.setLong(1, tid);
            stmt.setLong(2, bid);
            stmt.setLong(3, bid);
            stmt.setInt(4, delta);
            stmt.executeUpdate();
        }
        c.commit();
    }

    public String getSchemaPrefix() {
        return conf.getSchema() != null ? conf.getSchema() + "." : "";
    }

    public String getTablespaceClause() {
        return conf.getTablespace() != null ? " TABLESPACE " + conf.getTablespace() : "";
    }

    public abstract String getNologgingClause();

}
