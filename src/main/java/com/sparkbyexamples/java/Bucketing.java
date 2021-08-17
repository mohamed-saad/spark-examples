package com.sparkbyexamples.java;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;

public class Bucketing {

    private static void generateBucketing(SparkSession spark) {
        /*
            IMPORTANT
            .........
            The code here will not work because Spark "write" does not
            support saving buckets to files, the only way to do that is
            to save it as Hive table using "saveAsTable"
         */
        spark.read().json("src/main/resources/flight.json")
                .write()
                .bucketBy(5, "dst")
                .mode(SaveMode.Overwrite)
                .json("src/main/resources/flight-buck.json");
    }

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .getOrCreate();

        generateBucketing(spark);

        spark.read().json("src/main/resources/flight-buck.json")
                .groupBy(col("dst"))
                .count()
                .explain(true);

        spark.read().json("src/main/resources/flight.json")
                .groupBy(col("dst"))
                .count()
                .explain(true);
    }
}
