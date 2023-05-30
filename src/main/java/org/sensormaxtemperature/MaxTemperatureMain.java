package org.sensormaxtemperature;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MaxTemperatureMain {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

        sensorData
                .keyBy(sensorReading -> sensorReading.sensorId)
                .process(new MaxTemperatureProcessor())
                .print();

        env.execute("Max Temperature Example");
    }


}
