package org.customobjectmanipulationonkafkawithstate;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.List;
import java.util.Properties;

import static org.customobjectmanipulationwithkafka.KafkaConfig.BOOTSTRAP_SERVER;
import static org.customobjectmanipulationwithkafka.KafkaConfig.PRODUCT_IN_TOPIC;

public class MyKafkaProducer {

  private static Producer<String, Product> productProducer;

  public MyKafkaProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProductSerializer.class);
    productProducer = new KafkaProducer<>(props);
    Thread kafkaShutdown = new Thread(() -> productProducer.close());
    Runtime.getRuntime().addShutdownHook(kafkaShutdown);
  }

  public void send(List<Product> products) {
    products.forEach(product -> {
      ProducerRecord<String, Product> record = new ProducerRecord<>(PRODUCT_IN_TOPIC, product);
      productProducer.send(record);
    });
    productProducer.flush();
  }
}
