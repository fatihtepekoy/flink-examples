package org.avgtempwithtableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.sensor.SensorReading;
import org.sensor.SensorSource;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;


public class AvgTemperatureMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

        tableEnv.createTemporaryView(
                "SensorData",
                sensorData,
                Schema.newBuilder()
                        .column("sensorId", DataTypes.STRING())
                        .column("temperature", DataTypes.FLOAT())
                        .columnByExpression("processing_time", "PROCTIME()")
                        .build());

        Table avgTempTable = tableEnv.from("SensorData")
                .window(Tumble.over(lit(5).seconds()).on($("processing_time")).as("fiveSecondsWindow"))
                .groupBy($("sensorId"), $("fiveSecondsWindow"))
                .select($("sensorId"), $("fiveSecondsWindow").end().as("end_time"), $("temperature").avg().as("avg_temperature"));

        tableEnv.toDataStream(avgTempTable).print();

        env.execute("Average Temperature Calculation with Table API");
    }


}
