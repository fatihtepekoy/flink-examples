package org.customobjectmanipulationonkafkawithstate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ProductAmountAggregatorAndAlert extends
        ProcessFunction<Product, Tuple2<String, Integer>> {

    private transient ValueState<Integer> amountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("amountState", Integer.class);
        amountState = getRuntimeContext().getState(descriptor);
    }


    @Override
    public void processElement(Product product, ProcessFunction<Product, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
        Integer currentAmount = amountState.value();
        int newAmount = currentAmount != null ? currentAmount + product.getAmount() : product.getAmount();
        amountState.update(newAmount);

        if (newAmount > 9) {
            collector.collect(new Tuple2<>(product.getName(), newAmount));
            // callAlertFunction(....)
        }

    }
}
