package com.sparkbyexamples.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;

public class Partitioning {

    private static void generatePartitions(SparkSession spark) {
        spark.read().json("src/main/resources/flight.json")
                .write()
                .partitionBy("dofW", "carrier", "dst")
                .mode(SaveMode.Overwrite)
                .json("src/main/resources/flight-part.json");
    }

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .getOrCreate();

        generatePartitions(spark);

        spark.read().json("src/main/resources/flight-part.json")
                .filter(col("carrier").equalTo("DL"))
                .filter(col("dst").lt("BOS"))
                .filter(col("dofW").gt("4"))
                .explain(true);

        spark.read().json("src/main/resources/flight.json")
                .filter(col("carrier").equalTo("DL"))
                .filter(col("dst").lt("BOS"))
                .filter(col("dofW").gt("4"))
                .explain(true);
    }
}
