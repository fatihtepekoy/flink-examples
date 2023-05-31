package org.avgtempwithwindow;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sensor.SensorReading;

import java.sql.Timestamp;

public class AverageTempFunction extends ProcessWindowFunction<SensorReading, Tuple3<String, Double, String>, String, TimeWindow> {

    @Override
    public void process(String key, ProcessWindowFunction<SensorReading, Tuple3<String, Double, String>, String, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<Tuple3<String, Double, String>> out) throws Exception {
        int count = 0;
        double sum = 0.0;

        for (SensorReading reading : elements) {
            sum += reading.getTemperature();
            count++;
        }

        double average = sum / count;
        out.collect(new Tuple3<>(key, average, new Timestamp(context.window().getEnd()).toLocalDateTime().toString()));

    }
}
