package engine.dto;

public record BenchConf(
        DbEngine engine,
        String host,
        int port,
        String dbname,
        String username,
        String password,
        String schema,
        String tablespace,
        boolean nologging,
        int scale,
        int concurrency,
        int time,
        boolean readOnly) {
}
