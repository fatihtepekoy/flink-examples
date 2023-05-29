package org.customobjectmanipulationonkafkawithstate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class ProductKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<Product> {

    private ObjectMapper mapper = new ObjectMapper();
    private final String topic;

    public ProductKafkaRecordSerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Product product, KafkaSinkContext kafkaSinkContext, Long aLong) {
        byte[] b = null;
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            b = mapper.writeValueAsBytes(product);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new ProducerRecord<>(topic, b);
    }
}

