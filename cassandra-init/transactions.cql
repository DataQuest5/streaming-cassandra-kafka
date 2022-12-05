CREATE KEYSPACE testdb WITH replication = { 'class': 'SimpleStrategy',
'replication_factor': 3 }
AND durable_writes = true;
USE testdb;
CREATE TABLE transactions(
    id bigint PRIMARY KEY,
    account text,
    transaction text,
    day text,
    hash text,
) WITH cdc = true;

INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (1, 'A1', 'T1', 'd1', 'HV1');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (2, 'A1', 'T2', 'd2', 'HV2');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (3, 'A1', 'T3', 'd2', 'HV3');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (4, 'A2', 'T1', 'd1', 'HV4');
    INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (5, 'A3', 'T1', 'd4', 'HV5');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (6, 'A3', 'T2', 'd1', 'HV6');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (7, 'A3', 'T3', 'd2', 'HV7');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (8, 'A3', 'T4', 'd2', 'HV8');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (9, 'A3', 'T5', 'd1', 'HV9');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (10, 'A4', 'T1', 'd4', 'HV10');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (11, 'A4', 'T2', 'd5', 'HV11');
INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (12, 'A4', 'T3', 'd5', 'HV12');
