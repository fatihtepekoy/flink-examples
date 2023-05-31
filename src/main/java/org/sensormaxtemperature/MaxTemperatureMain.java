package org.sensormaxtemperature;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sensor.SensorReading;
import org.sensor.SensorSource;

public class MaxTemperatureMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

        sensorData
                .keyBy(SensorReading::getSensorId)
                .process(new MaxTemperatureProcessor())
                .print();

        env.execute("Max Temperature Example");
    }


}
