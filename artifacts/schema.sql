-- ddl oracle
DROP TABLE bench_branches CASCADE CONSTRAINTS PURGE;
CREATE TABLE bench_branches (bid NUMBER(38,0) NOT NULL, bbalance NUMBER(38,0)) NOLOGGING TABLESPACE tblspace;

DROP TABLE bench_tellers CASCADE CONSTRAINTS PURGE;
CREATE TABLE bench_tellers (tid NUMBER(38,0) NOT NULL, bid NUMBER(38,0) NOT NULL, tbalance NUMBER(38,0)) NOLOGGING TABLESPACE tblspace;

DROP TABLE bench_accounts CASCADE CONSTRAINTS PURGE;
CREATE TABLE bench_accounts (aid NUMBER(38,0) NOT NULL, bid NUMBER(38,0) NOT NULL, abalance NUMBER(38,0)) NOLOGGING TABLESPACE tblspace;

DROP TABLE bench_history CASCADE CONSTRAINTS PURGE;
CREATE TABLE bench_history (tid number(38,0) NOT NULL, bid number(38,0) NOT NULL, aid number(38,0) NOT NULL, delta number(38,0), mtime timestamp(6)) NOLOGGING TABLESPACE tblspace;

-- ddl postgres
DROP TABLE IF EXISTS bench_branches CASCADE;
CREATE UNLOGGED TABLE bench_branches (bid INTEGER NOT NULL, bbalance INTEGER) TABLESPACE tblspace;

DROP TABLE IF EXISTS bench_tellers CASCADE;
CREATE UNLOGGED TABLE bench_tellers (tid INTEGER NOT NULL, bid INTEGER NOT NULL, tbalance INTEGER) TABLESPACE tblspace;

DROP TABLE IF EXISTS bench_accounts CASCADE;
CREATE UNLOGGED TABLE bench_accounts (aid INTEGER NOT NULL, bid INTEGER NOT NULL, abalance INTEGER) TABLESPACE tblspace;

DROP TABLE IF EXISTS bench_history CASCADE;
CREATE UNLOGGED TABLE bench_history (tid INTEGER NOT NULL, bid INTEGER NOT NULL, aid INTEGER NOT NULL, delta INTEGER, mtime TIMESTAMP(6)) TABLESPACE tblspace;

-- indexes
CREATE UNIQUE INDEX bench_branches_pk ON bench_branches (bid) TABLESPACE tblspace;
CREATE UNIQUE INDEX bench_tellers_pk ON bench_tellers (tid) TABLESPACE tblspace;
CREATE UNIQUE INDEX bench_accounts_pk ON bench_accounts (aid) TABLESPACE tblspace;
