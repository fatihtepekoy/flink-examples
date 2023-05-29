package org.customobjectmanipulationonkafkawithstate;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // prepare test data and write the input topic
        PublishInitialTestData.publish();
        // read the input topic
        KafkaSource<Product> kafkaSource = getProductKafkaSource();
        // Add the Kafka source to the Flink application
        DataStreamSource<Product> dataStreamSource = getProductDataStreamSource(env, kafkaSource);

        // Distribute the data based on the product(Product class hashCode is overwritten)
        KeyedStream<Product, Product> productObjectKeyedStream = getProductObjectKeyedStream(dataStreamSource);

//        SingleOutputStreamOperator<Product> sum = productObjectKeyedStream.sum(1);

        SingleOutputStreamOperator<Product> sumOfProductAmountsStream = getOutputStreamOperator(productObjectKeyedStream);

        // Prepare sink
        KafkaSink<Product> sink = getProductKafkaSink();
        // Add the sink to the outStream
        sumOfProductAmountsStream.sinkTo(sink);
        // Execute the Flink job
        env.execute("Product sum of amount example");
    }

    private static SingleOutputStreamOperator<Product> getOutputStreamOperator(KeyedStream<Product, Product> productObjectKeyedStream) {
        return productObjectKeyedStream.map(new ProductAmountSumStatefulOperation());
    }

    private static KeyedStream<Product, Product> getProductObjectKeyedStream(DataStreamSource<Product> dataStreamSource) {
        return dataStreamSource.keyBy(product -> product);
    }


    private static KafkaSink<Product> getProductKafkaSink() {
        return KafkaSink.<Product>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVER)
                .setRecordSerializer(new ProductKafkaRecordSerializationSchema(KafkaConfig.PRODUCT_OUT_TOPIC))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static DataStreamSource<Product> getProductDataStreamSource(StreamExecutionEnvironment env, KafkaSource<Product> kafkaSource) {
        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
                "kafka source product topic input");
    }

    private static KafkaSource<Product> getProductKafkaSource() {
        return KafkaSource.<Product>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVER)
                .setTopics(KafkaConfig.PRODUCT_IN_TOPIC)
                .setValueOnlyDeserializer(new ProductDeserializer())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
    }
}
