package org.customobjectmanipulationonkafkawithstate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class ProductAmountAggregator implements AggregateFunction<Product, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<>("", 0);
    }

    @Override
    public Tuple2<String, Integer> add(Product product, Tuple2<String, Integer> accumulator) {
        return new Tuple2<>(product.getName(), accumulator.f1 + product.getAmount());
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
        return new Tuple2<>(a.f0, a.f1 + b.f1);
    }
}

