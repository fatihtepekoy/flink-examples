
# Custom object(Pojo class) manipulation with Apache Flink using Kafka sources

// Before you run the main method you need Kafka server and some topics
// You may use single instance kafka without Zookeper

// PublishInitialTestData reads data file under resources and write to the product-in-topic to generate test data


```
docker run --name kafka -p 9092:9092 -d bashj79/kafka-kraft
```

// connect to the Kafka instance
```
winpty docker exec -it kafka //bin//sh
```

// create product-in-topic
```
/opt/kafka/bin/kafka-topics.sh --create  --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092 --topic product-in-topic
```

// create product-out-topic
```
/opt/kafka/bin/kafka-topics.sh --create  --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092 --topic product-out-topic
```

//Check the results if the all product's amounts are zero
```
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic product-out-topic --from-beginning
```



