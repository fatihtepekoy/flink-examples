package org.avgsensorvalueinwindowwithstate;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.LocalDateTime;

public class AvgTemperatureMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> sensorData = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.timestamp));


        DataStream<Tuple3<String, Double, LocalDateTime>> outputStream = sensorData
                .keyBy(SensorReading::getSensorId)
                .process(new AverageTempFunctionBasedOnProcessingTimestamp());

        outputStream.print();

        env.execute("Max Temperature Example");
    }


}
