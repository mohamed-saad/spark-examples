package com.sparkbyexamples.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class DataframeTest {

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .getOrCreate();
        Dataset<Row> flight = spark.read().json("src/main/resources/flight.json").repartition(8);
//        System.out.println(flight.count()); // 282628
        flight
            .filter("dist > 2700")
            .groupBy("carrier")
            .count()
            .orderBy("count")
            .show();   // 3221
        System.in.read();   // Now open http://localhost:4040/SQL/execution/?id=0
    }
}
