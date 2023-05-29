package org.customobjectmanipulationwithkafka;

import java.util.List;

public class PublishInitialTestData {

    public static void publish() {
        List<Product> products = ProductFileReader.getInstance().getProducts();
        MyKafkaProducer myKafkaProducer = new MyKafkaProducer();
        myKafkaProducer.send(products);
    }

}
