package org.sensor;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SensorSource implements SourceFunction<SensorReading> {

    private volatile boolean running = true;
    private final Random rand = new Random();

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        String[] sensors = {"sensor1", "sensor2", "sensor3"};
        while (running) {
            String sensor = sensors[rand.nextInt(sensors.length)];
            float temperature = rand.nextFloat() * 100;
            long timestamp = System.currentTimeMillis();
            ctx.collect(new SensorReading(sensor, temperature, timestamp));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
