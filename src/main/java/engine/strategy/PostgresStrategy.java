package engine.strategy;

import engine.dto.BenchConf;
import lombok.extern.log4j.Log4j2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static engine.utils.CommonUtils.smartElapsed;

@Log4j2
public class PostgresStrategy extends DatabaseStrategy {

    private static final String JDBC_URL_TEMPLATE = "jdbc:postgresql://%s:%d/%s";

    private static final String DROP_TABLE_STMT = "DROP TABLE IF EXISTS %s%s CASCADE";
    private static final String ANALYZE_TABLE_STMT = "VACUUM ANALYZE %s%s";

    private static final String CREATE_BRANCHES_STMT = "CREATE %s TABLE %sbench_branches (bid INTEGER NOT NULL, bbalance INTEGER) %s";
    private static final String CREATE_TELLERS_STMT = "CREATE %s TABLE %sbench_tellers (tid INTEGER NOT NULL, bid INTEGER NOT NULL, tbalance INTEGER) %s";
    private static final String CREATE_ACCOUNTS_STMT = "CREATE %s TABLE %sbench_accounts (aid INTEGER NOT NULL, bid INTEGER NOT NULL, abalance INTEGER) %s";
    private static final String CREATE_HISTORY_STMT = "CREATE %s TABLE %sbench_history (tid INTEGER NOT NULL, bid INTEGER NOT NULL, aid INTEGER NOT NULL, delta INTEGER, mtime TIMESTAMP(6)) %s";

    public PostgresStrategy(BenchConf conf) throws ClassNotFoundException {
        super(conf);
        Logger logger = Logger.getLogger("org.postgresql");
        logger.setLevel(Level.OFF);
        Class.forName("org.postgresql.Driver");
    }

    @Override
    public Connection doConnect() throws SQLException {
        String jdbcUrl = String.format(JDBC_URL_TEMPLATE, conf.getHost(), conf.getPort(), conf.getDbname());
        Properties props = new Properties();
        props.setProperty("user", conf.getUsername());
        if (conf.getPassword() != null) {
            props.setProperty("password", conf.getPassword());
        }
        Connection c = DriverManager.getConnection(jdbcUrl, props);
        c.setAutoCommit(false);
        return c;
    }

    @Override
    public void dropTables(Connection c) throws SQLException {
        log.info("Dropping tables...");
        c.setAutoCommit(true);
        long startTime = System.nanoTime();
        for (String table : tables) {
            String sql = String.format(DROP_TABLE_STMT, getSchemaPrefix(), table);
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }
        long endTime = System.nanoTime();
        c.setAutoCommit(false);
        log.info("done! ({})\n", smartElapsed(endTime - startTime));
    }

    @Override
    public void createTables(Connection c) throws SQLException {
        log.info("Creating tables...");
        c.setAutoCommit(true);
        long startTime = System.nanoTime();
        String sql = String.format(CREATE_BRANCHES_STMT, getNologgingClause(), getSchemaPrefix(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        sql = String.format(CREATE_TELLERS_STMT, getNologgingClause(), getSchemaPrefix(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        sql = String.format(CREATE_ACCOUNTS_STMT, getNologgingClause(), getSchemaPrefix(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        sql = String.format(CREATE_HISTORY_STMT, getNologgingClause(), getSchemaPrefix(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        long endTime = System.nanoTime();
        c.setAutoCommit(false);
        log.info("done! ({})\n", smartElapsed(endTime - startTime));
    }

    @Override
    public void analyzeTables(Connection c) throws SQLException {
        log.info("Analyzing...");
        c.setAutoCommit(true);
        long startTime = System.nanoTime();
        for (String table : tables) {
            String sql = String.format(ANALYZE_TABLE_STMT, getSchemaPrefix(), table);
            try (Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }
        long endTime = System.nanoTime();
        c.setAutoCommit(false);
        log.info("done! ({})\n", smartElapsed(endTime - startTime));
    }

    @Override
    public String getNologgingClause() {
        return conf.isNologging() ? "UNLOGGED" : "";
    }

}
