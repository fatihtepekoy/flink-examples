package org.customobjectmanipulationwithkafka;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Product {

    @JsonProperty("name")
    private String name;

    @JsonProperty("amount")
    private int amount;

    public String getName() {
        return name;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Product{" +
                "name='" + name + '\'' +
                ", amount=" + amount +
                '}';
    }
}
