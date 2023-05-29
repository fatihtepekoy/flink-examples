package org.customobjectmanipulationonkafkawithstate;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Product {

  @JsonProperty("name")
  private String name;

  @JsonProperty("amount")
  private int amount;

  public Product(String name, int amount) {
    this.name = name;
    this.amount = amount;
  }

  public Product() {
  }

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Product)) return false;

    Product product = (Product) o;

    return getName().equals(product.getName());
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }
}
