package org.avgsensorvalueinwindowwithstate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

public class AverageTempFunctionBasedOnProcessingTimestamp extends KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, LocalDateTime>> {

    private ValueState<SumCount> sumCountState;


    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialize the ValueState descriptor
        ValueStateDescriptor<SumCount> descriptor = new ValueStateDescriptor<>("sumCountState", SumCount.class);
        sumCountState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, LocalDateTime>>.Context ctx, Collector<Tuple3<String, Double, LocalDateTime>> out) throws Exception {
        SumCount currentSumCount = sumCountState.value();

        if (currentSumCount == null) {
            currentSumCount = new SumCount(0.0, 0);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
        }

        // Update the state with the new value
        double newSum = currentSumCount.getSum() + value.temperature;
        int newCount = currentSumCount.getCount() + 1;
        SumCount newSumCount = new SumCount(newSum, newCount);

        // Store the updated state
        sumCountState.update(newSumCount);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Double, LocalDateTime>> out) throws Exception {
        // Retrieve the current state for the key
        SumCount currentSumCount = sumCountState.value();

        // Calculate the average value
        double average = currentSumCount.getSum() / currentSumCount.getCount();

        // Emit the result
        out.collect(new Tuple3<>(ctx.getCurrentKey(), average, LocalDateTime.now()));

        // Clear the state
        sumCountState.clear();
    }

}
