package org.customobjectmanipulationwithkafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

public class ProductDeserializer implements DeserializationSchema<Product> {

    @Override
    public Product deserialize(byte[] bytes) {
        Product product = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            product = objectMapper.readValue(bytes, Product.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return product;
    }

    @Override
    public void deserialize(byte[] message, Collector<Product> out) {
        Product product = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            product = objectMapper.readValue(message, Product.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        out.collect(product);
    }

    @Override
    public boolean isEndOfStream(Product product) {
        return false;
    }

    @Override
    public TypeInformation<Product> getProducedType() {
        return TypeInformation.of(Product.class);
    }

}

