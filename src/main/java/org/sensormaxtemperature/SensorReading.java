package org.sensormaxtemperature;

public class SensorReading {
    public String sensorId;
    public float temperature;

    public SensorReading(String sensorId, float temperature) {
        this.sensorId = sensorId;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "sensorId='" + sensorId + '\'' +
                ", temperature=" + temperature +
                '}';
    }
}
