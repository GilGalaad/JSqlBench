package engine.strategy;

import static engine.CommonUtils.smartElapsed;
import engine.dto.BenchConf;
import static engine.strategy.DatabaseStrategy.tables;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class OracleStrategy extends DatabaseStrategy {

    private static final String JDBC_URL_TEMPLATE = "jdbc:oracle:thin:@//%s:%d/%s";

    private static final String DROP_TABLE_STMT = "DROP TABLE %s%s CASCADE CONSTRAINTS PURGE";
    private static final String ANALYZE_TABLE_STMT = "EXECUTE dbms_stats.gather_table_stats(ownname => %s, tabname => '%s', estimate_percent => dbms_stats.auto_sample_size, degree=> dbms_stats.auto_degree, granularity => 'ALL')";

    private static final String CREATE_BRANCHES_STMT = "CREATE TABLE %sbench_branches (bid NUMBER(38,0) NOT NULL, bbalance NUMBER(38,0)) %s %s";
    private static final String CREATE_TELLERS_STMT = "CREATE TABLE %sbench_tellers (tid NUMBER(38,0) NOT NULL, bid NUMBER(38,0) NOT NULL, tbalance NUMBER(38,0)) %s %s";
    private static final String CREATE_ACCOUNTS_STMT = "CREATE TABLE %sbench_accounts (aid NUMBER(38,0) NOT NULL, bid NUMBER(38,0) NOT NULL, abalance NUMBER(38,0)) %s %s";
    private static final String CREATE_HISTORY_STMT = "CREATE TABLE %sbench_history (tid number(38,0) NOT NULL, bid number(38,0) NOT NULL, aid number(38,0) NOT NULL, delta number(38,0), mtime timestamp(6)) %s %s";

    public OracleStrategy(BenchConf conf) throws ClassNotFoundException {
        super(conf);
        Class.forName("oracle.jdbc.OracleDriver");
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
            } catch (SQLException ex) {
                // ignoring error ORA-00942: table or view does not exist
                if (ex.getErrorCode() != 942) {
                    throw ex;
                }
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
        String sql = String.format(CREATE_BRANCHES_STMT, getSchemaPrefix(), getNologgingClause(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        sql = String.format(CREATE_TELLERS_STMT, getSchemaPrefix(), getNologgingClause(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        sql = String.format(CREATE_ACCOUNTS_STMT, getSchemaPrefix(), getNologgingClause(), getTablespaceClause()).trim();
        try (Statement stmt = c.createStatement()) {
            stmt.execute(sql);
        }
        sql = String.format(CREATE_HISTORY_STMT, getSchemaPrefix(), getNologgingClause(), getTablespaceClause()).trim();
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
            String sql = String.format(ANALYZE_TABLE_STMT, conf.getSchema() == null ? "user" : "'" + conf.getSchema().toUpperCase() + "'", table.toUpperCase());
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
        return conf.isNologging() ? "NOLOGGING" : "";
    }

}
