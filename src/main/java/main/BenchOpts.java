package main;

public class BenchOpts {

    public static enum DbEngine {
        ORACLE,
        POSTGRES
    }

    private DbEngine engine;
    private String hostname;
    private int port;
    private String dbname;
    private String username;
    private String password;
    private String schema;
    private String tablespace;
    private boolean nologging = false;
    private int scale = 1;
    private int concurrency = 1;
    private int time;

    public DbEngine getEngine() {
        return engine;
    }

    protected void setEngine(DbEngine engine) {
        this.engine = engine;
    }

    public String getHostname() {
        return hostname;
    }

    protected void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    protected void setPort(int port) {
        this.port = port;
    }

    public String getDbname() {
        return dbname;
    }

    protected void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public String getUsername() {
        return username;
    }

    protected void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    protected void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    protected void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTablespace() {
        return tablespace;
    }

    protected void setTablespace(String tablespace) {
        this.tablespace = tablespace;
    }

    public boolean isNologging() {
        return nologging;
    }

    protected void setNologging(boolean nologging) {
        this.nologging = nologging;
    }

    public int getScale() {
        return scale;
    }

    protected void setScale(int scale) {
        this.scale = scale;
    }

    public int getConcurrency() {
        return concurrency;
    }

    protected void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getTime() {
        return time;
    }

    protected void setTime(int time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "BenchOpts{" + "engine=" + engine + ", hostname=" + hostname + ", port=" + port + ", dbname=" + dbname + ", username=" + username + ", password=" + password + ", schema=" + schema + ", tablespace=" + tablespace + ", nologging=" + nologging + ", scale=" + scale + ", concurrency=" + concurrency + ", time=" + time + '}';
    }

}
