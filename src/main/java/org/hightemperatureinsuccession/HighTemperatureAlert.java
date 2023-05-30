package org.hightemperatureinsuccession;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HighTemperatureAlert {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

        DataStream<String> outputStream = sensorData
                .keyBy(sensorReading -> sensorReading.sensorId)
                .process(new HighTemperatureProcessor(20.0f, 3));

        outputStream.print();

        env.execute("High Temperature Alert");
    }


}
