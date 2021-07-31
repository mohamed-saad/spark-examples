package com.sparkbyexamples.java;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DataframeTest {

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .getOrCreate();
        Dataset<Row> flight = spark.read().json("src/main/resources/flight.json");
        System.out.println(flight.count());

        System.in.read();   // Now open http://localhost:4040/
    }
}
