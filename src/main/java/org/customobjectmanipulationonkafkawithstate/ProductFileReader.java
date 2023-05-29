package org.customobjectmanipulationonkafkawithstate;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class ProductFileReader {

    private static final ProductFileReader PRODUCT_FILE_READER = new ProductFileReader();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String DATAFILE = "src/main/resources/data.json";

    public static ProductFileReader getInstance() {
        return PRODUCT_FILE_READER;
    }

    public List<Product> getProducts() {
        try {
            return Arrays.asList(mapper.readValue(new File(DATAFILE), Product[].class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
