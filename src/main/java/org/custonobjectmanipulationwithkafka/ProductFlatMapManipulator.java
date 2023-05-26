package org.custonobjectmanipulationwithkafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class ProductFlatMapManipulator implements FlatMapFunction<Product, Product> {
    @Override
    public void flatMap(Product product, Collector<Product> collector) {
        product.setAmount(0);
        collector.collect(product);
    }
}