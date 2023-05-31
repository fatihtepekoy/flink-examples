package org.hightemperatureinsuccession;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sensor.SensorReading;

public class HighTemperatureProcessor extends KeyedProcessFunction<String, SensorReading, String> {

    private transient ValueState<Integer> highTempCountState;

    private final float threshold;
    private final int maxHighTempCount;

    public HighTemperatureProcessor(float threshold, int maxHighTempCount) {
        this.threshold = threshold;
        this.maxHighTempCount = maxHighTempCount;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        highTempCountState = getRuntimeContext().getState(new ValueStateDescriptor<>("highTempCount", TypeInformation.of(Integer.class)));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
        Integer currentCount = highTempCountState.value();
        if (currentCount == null) {
            currentCount = 0;
        }

        if (value.getTemperature() > threshold) {
            currentCount++;
        } else {
            currentCount = 0;
        }

        if (currentCount >= maxHighTempCount) {
            out.collect("Sensor " + value.getSensorId() + " has recorded " + maxHighTempCount + " consecutive temperatures above " + threshold + ".");
            currentCount = 0;
        }

        highTempCountState.update(currentCount);
    }
}
