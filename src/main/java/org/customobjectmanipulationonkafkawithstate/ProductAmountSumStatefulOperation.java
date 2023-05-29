package org.customobjectmanipulationonkafkawithstate;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

public class ProductAmountSumStatefulOperation extends RichMapFunction<Product, Product> {
    ValueState<Integer> totalAmountOfTheProduct;

    @Override
    public void open(Configuration conf) {
        ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("Sum of product amounts", Integer.class);
        totalAmountOfTheProduct = getRuntimeContext().getState(desc);
    }

    @Override
    public Product map(Product product) throws Exception {
        totalAmountOfTheProduct.update(totalAmountOfTheProduct.value() == null ? product.getAmount() : totalAmountOfTheProduct.value() + product.getAmount());
        return new Product(product.getName(), totalAmountOfTheProduct.value());
    }

}