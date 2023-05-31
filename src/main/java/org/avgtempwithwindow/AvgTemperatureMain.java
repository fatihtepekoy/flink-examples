package org.avgtempwithwindow;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sensor.SensorReading;
import org.sensor.SensorSource;

import java.time.Duration;

public class AvgTemperatureMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> sensorData = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));


        DataStream<Tuple3<String, Double, String>> outputStream = sensorData
                .keyBy(SensorReading::getSensorId).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new AverageTempFunction());

        outputStream.print();

        env.execute("Avg Temperature Example with window");
    }


}
