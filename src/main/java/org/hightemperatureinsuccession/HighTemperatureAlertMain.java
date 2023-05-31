package org.hightemperatureinsuccession;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sensor.SensorReading;
import org.sensor.SensorSource;

public class HighTemperatureAlertMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

        DataStream<String> outputStream = sensorData
                .keyBy(SensorReading::getSensorId)
                .process(new HighTemperatureProcessor(20.0f, 3));

        outputStream.print();

        env.execute("High Temperature Alert");
    }


}
