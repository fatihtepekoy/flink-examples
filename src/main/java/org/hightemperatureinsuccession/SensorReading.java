package org.hightemperatureinsuccession;

public class SensorReading {
    public String sensorId;
    public float temperature;

    public SensorReading(String sensorId, float temperature) {
        this.sensorId = sensorId;
        this.temperature = temperature;
        System.out.println(this);
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "sensorId='" + sensorId + '\'' +
                ", temperature=" + temperature +
                '}';
    }
}
