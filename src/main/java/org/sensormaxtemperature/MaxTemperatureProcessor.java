package org.sensormaxtemperature;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sensor.SensorReading;

public class MaxTemperatureProcessor extends KeyedProcessFunction<String, SensorReading, SensorReading> {

    private transient ValueState<Float> maxTemperatureState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Float> maxTemperature = new ValueStateDescriptor<>("maxTemperature", TypeInformation.of(Float.class));
        maxTemperatureState = getRuntimeContext().getState(maxTemperature);
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
        Float currentMaxTemperature = maxTemperatureState.value();
        if (currentMaxTemperature == null || value.getTemperature() > currentMaxTemperature) {
            maxTemperatureState.update(value.getTemperature());
            out.collect(value);
        }
    }
}
