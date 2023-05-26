
# Simple string manipulation with Apache Flink using Kafka sources

// Before you run the main method you need Kafka server and some topics

// you may use single instance kafka without zookeper

```
docker run --name kafka -p 9092:9092 -d bashj79/kafka-kraft
```

// connect to the Kafka instance
```
winpty docker exec -it kafka //bin//sh
```

// creta string-in-topic
```
/opt/kafka/bin/kafka-topics.sh --create  --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092 --topic string-in-topic
```
// write some test strings with spaces , CTRL+D to stop
```
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic string-in-topic
```

//Check the results if the strings split and written to the out topic separately
```
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic string-out-topic --from-beginning
```


