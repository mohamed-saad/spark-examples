package com.sparkbyexamples.java;

import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;

import java.io.IOException;

public class DatasetTest {

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .getOrCreate();
        ExpressionEncoder<Flight> encoder = (ExpressionEncoder<Flight>) Encoders.bean(Flight.class);
        Dataset<Flight> dataset = spark.read().json("src/main/resources/flight.json").repartition(8).as(encoder);
        dataset.filter((FilterFunction<Flight>)(flight -> flight.getDist() > 2700))
                .groupByKey((MapFunction<Flight, String>)(flight -> flight.getCarrier()), Encoders.STRING())
                .count().withColumnRenamed("count(1)", "counts")
                .orderBy(col("counts"))
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("/tmp/flight.parquet");
        System.in.read();   // Now open http://localhost:4040/SQL/execution/?id=0
    }
}
