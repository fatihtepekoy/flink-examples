package org.basicstringmanipulationonkafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.stream.Stream;

public class WordSplitter implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String line, Collector<String> out) {
        Stream<String> words = Arrays.stream(line.split("\\s+"));
        words.forEach(out::collect);
    }
}