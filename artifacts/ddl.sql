-- create tables oracle
drop table schema.bench_branches cascade contraints purge;
create table schema.bench_branches (bid number(38,0) not null, bbalance number(38,0)) nologging tablespace tblspace;

drop table schema.bench_tellers cascade contraints purge;
create table schema.bench_tellers (tid number(38,0) not null, bid number(38,0), tbalance number(38,0)) nologging tablespace tblspace;

drop table schema.bench_accounts cascade contraints purge;
create table schema.bench_accounts (aid number(38,0) not null, bid number(38,0), abalance number(38,0)) nologging tablespace tblspace;

drop table schema.bench_history cascade contraints purge;
create table schema.bench_history (tid number(38,0), bid number(38,0), aid number(38,0), delta number(38,0), mtime timestamp(6)) nologging tablespace tblspace;

-- create tables postgres
drop table if exists schema.bench_branches cascade;
create unlogged table schema.bench_branches (bid integer not null, bbalance integer) tablespace tblspace;

drop table if exists schema.bench_tellers cascade;
create unlogged table schema.bench_tellers (tid integer not null, bid integer, tbalance integer) tablespace tblspace;

drop table if exists schema.bench_accounts cascade;
create unlogged table schema.bench_accounts (aid integer not null, bid integer, abalance integer) tablespace tblspace;

drop table if exists schema.bench_history cascade;
create unlogged table schema.bench_history (tid integer, bid integer, aid integer, delta integer, mtime timestamp(6)) tablespace tblspace;

-- indexes
create unique index pk_bench_branches on schema.bench_branches (bid) tablespace tblspace;
create unique index pk_bench_tellers on schema.bench_tellers (tid) tablespace tblspace;
create unique index pk_bench_accounts on schema.bench_accounts (aid) tablespace tblspace;
