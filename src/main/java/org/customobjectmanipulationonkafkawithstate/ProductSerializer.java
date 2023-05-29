package org.customobjectmanipulationonkafkawithstate;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class ProductSerializer implements Serializer<Product> {


    @Override
    public byte[] serialize(String s, Product product) {
        byte[] serializedData = null;
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            serializedData = objectMapper.writeValueAsBytes(product);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return serializedData;
    }
}
