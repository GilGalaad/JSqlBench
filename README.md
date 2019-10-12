# JSqlBench
Simple Java program for running benchmarks on RDBMS.

Inspired by [pg_bench](https://www.postgresql.org/docs/devel/static/pgbench.html), this software emulates a simple OLTP workload and measures TPS (transactions per second), average latency and standard deviation. Currently supported RDBMS are Oracle and PostgreSQL, but the _Strategy_ design pattern used makes it easy to cover other engines.

#### Usage
```
$ java -jar JSqlBench.jar --help
Usage: JSqlBench [OPTIONS]
      --engine=<engine>   Database engine. Currently supported: Oracle and
                            Postgres
      --host=<host>       Database server's hostname (default: localhost)
      --port=<port>       Database server's port (default: 1521 for Oracle and
                            5432 for Postgres)
      --dbname=<dbname>   Database or instance name (SID)
      --username=<username>
                          Username used to log in
      --password=<password>
                          Password used to log in
      --schema=<schema>   Create objects in the specified namespace or schema,
                            rather than the default one
      --tablespace=<tablespace>
                          Create objects in the specified tablespace, rather
                            than the default one
      --nologging         Create tables in nologging mode
      --scale=<scale>     Initialization scale factor, 1 = 100.000 rows. The
                            initialization scale factor should be at least as
                            large as the largest number of clients you intend
                            to test, else you'll mostly be measuring update
                            contention (default: 1)
      --concurrency=<concurrency>
                          Number of concurrent clients simulated (default: 1)
      --time=<time>       Run the test for this many seconds. Never believe any
                            test that runs for only a few seconds, it is a good
                            practice to make the run last at least a few
                            minutes. In some cases you could need hours to get
                            numbers that are reproducible (default: 300)
      --read-only         Simulate a read only worlkoad
      --help              Print this help and exit
 ```
