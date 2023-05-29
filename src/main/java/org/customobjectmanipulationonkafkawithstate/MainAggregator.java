//package org.customobjectmanipulationonkafkawithstate;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.connector.base.DeliveryGuarantee;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class MainAggregator {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.getExecutionEnvironment();
//        // prepare test data and write the input topic
//        PublishInitialTestData.publish();
//        // read the input topic
//        KafkaSource<Product> kafkaSource = getProductKafkaSource();
//
//        // Add the Kafka source to the Flink application
//        DataStreamSource<Product> dataStreamSource = getProductDataStreamSource(env, kafkaSource);
//
//        // group the stream by product name
//        DataStream<Tuple2<String, Integer>> resultStream = dataStreamSource
//                .keyBy(Product::getName)
//                .process(new ProductAmountAggregator());        // Execute the Flink job
//        env.execute("Product sum of amount example");
//    }
//
//    private static SingleOutputStreamOperator<Product> getOutputStreamOperator(KeyedStream<Product, Product> productObjectKeyedStream) {
//        return productObjectKeyedStream.map(new ProductAmountSumStatefulOperation());
//    }
//
//    private static KeyedStream<Product, Product> getProductObjectKeyedStream(DataStreamSource<Product> dataStreamSource) {
//        return dataStreamSource.keyBy(product -> product);
//    }
//
//
//    private static KafkaSink<Product> getProductKafkaSink() {
//        return KafkaSink.<Product>builder()
//                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVER)
//                .setRecordSerializer(new ProductKafkaRecordSerializationSchema(KafkaConfig.PRODUCT_OUT_TOPIC))
//                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                .build();
//    }
//
//    private static DataStreamSource<Product> getProductDataStreamSource(StreamExecutionEnvironment env, KafkaSource<Product> kafkaSource) {
//        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
//                "kafka source product topic input");
//    }
//
//    private static KafkaSource<Product> getProductKafkaSource() {
//        return KafkaSource.<Product>builder()
//                .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVER)
//                .setTopics(KafkaConfig.PRODUCT_IN_TOPIC)
//                .setValueOnlyDeserializer(new ProductDeserializer())
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .build();
//    }
//}
