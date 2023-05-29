package org.customobjectmanipulationwithkafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
        // Manipulate the data
        DataStream<Product> productOutStream = getProductDataOutStream(dataStreamSource);
        // Prepare sink
        KafkaSink<Product> sink = getProductKafkaSink();
        // Add the sink to the outStream
        productOutStream.sinkTo(sink);
        // Execute the Flink job
        env.execute("Pojo class manipulation example");
    }


    private static KafkaSink<Product> getProductKafkaSink() {
        return KafkaSink.<Product>builder()
                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVER)
                .setRecordSerializer(new ProductKafkaRecordSerializationSchema(KafkaConfig.PRODUCT_OUT_TOPIC))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static DataStream<Product> getProductDataOutStream(DataStreamSource<Product> dataStreamSource) {
        return dataStreamSource.flatMap(new ProductFlatMapManipulator());
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
