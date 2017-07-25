# JSqlBench
Simple Java program for running benchmarks on RDBMS.

Inspired by [pg_bench](https://www.postgresql.org/docs/devel/static/pgbench.html), this software emulates a simple OLTP workload and measures TPS (transactions per second) and average latency. Currently supported RDBMS are Oracle and PostgreSQL, but the modularity strategy used makes it easy to cover other engines.

#### Usage
```
usage: JSqlBench
 -c,--concurrency <dop>   Number of concurrent clients simulated (Degree
                          Of Parallelism) [default 1]
 -db,--database <db>      Database or instance name (SID)
 -e,--engine <eng>        Database engine
                          Currently supported: "oracle" or "postgres"
 -h,--hostname <host>     Database server's host name
 -nl,--nologging          Create tables in nologging mode
 -p,--port <port>         Database server's port number
 -P,--password <pwd>
 -S,--schema <schema>     Create tables in the specified namespace or
                          schema, rather than the default one
 -s,--scale <scale>       Initialization scale factor, 1 = 100.000 rows
                          The initialization scale factor (-s) should be
                          at least as large as the largest number of
                          clients you intend to test (-c), else you'll
                          mostly be measuring update contention
                          [default 1]
 -T,--tablespace <tbsp>   Create tables in the specified tablespace,
                          rather than the default tablespace
 -t,--time <dop>          Run the test for this many seconds
 -U,--username <user>
 ```
