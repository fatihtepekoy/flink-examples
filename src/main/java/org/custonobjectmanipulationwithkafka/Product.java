package org.custonobjectmanipulationwithkafka;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Product {

  @JsonProperty("colour")
  private String colour;

  @JsonProperty("amount")
  private int amount;

  public String getColour() {
    return colour;
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
            "colour='" + colour + '\'' +
            ", amount=" + amount +
            '}';
  }
}
