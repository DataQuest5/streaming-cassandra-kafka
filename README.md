# Cassandra <> Kafka CDC
The example project is built to showcase the CDC for Cassandra tables with Apache Kafka. As an integration between Cassandra and Kafka the example uses [debezium](https://debezium.io/).

Additionally you can check:
- [Trigger based CDC](https://smartcat.io/blog/data-engineering/cassandra-to-kafka-data-pipeline-part-1/)
- [Cassandra CDC with custom cdc consumer](https://smartcat.io/blog/data-engineering/cassandra-to-kafka-data-pipeline-part-2/)

## How to start the project

Clone and build the docker containers

```shell {cmd=sh}
git clone git@github.com:htimur/cassandra-cdc.git
cd cassandra-cdc
docker-compose up --build -d
```

This will pull/build the containers and create the example `transactions` table.

## Access the jupyter
```shell {cmd=sh}
docker-compose logs jupyter --follow
```

You'll see the jupyter link that you can use to login to the interface.


## Start the Kafka console consumer in separate terminal:

```shell {cmd=sh}
docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --from-beginning \
    --property print.key=true \
    --topic test_prefix.testdb.transactions
```

## Modify transactions via Cassandra client:

> note the TX logs will only be flushed out,
and thus be picked up by the connector, after accumulating 1 MB of changes


```shell {cmd=sh}
docker-compose exec cassandra bash -c 'cqlsh --keyspace=testdb'

INSERT INTO transactions(id, account, transaction, day, hash)
    VALUES (13, 'A4', 'T4', 'd6', 'HV13');

UPDATE transactions set day = 'd5' where id = 13;
DELETE FROM transactions WHERE id = 13;
```

