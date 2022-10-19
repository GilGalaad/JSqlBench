package engine.dto;

import lombok.Data;

@Data
public class BenchConf {

    public enum DbEngine {
        ORACLE,
        POSTGRES
    }

    private DbEngine engine;
    private String host;
    private int port;
    private String dbname;
    private String username;
    private String password;
    private String schema;
    private String tablespace;
    private boolean nologging;
    private int scale = 1;
    private int concurrency = 1;
    private int time;
    private boolean readOnly;

}
