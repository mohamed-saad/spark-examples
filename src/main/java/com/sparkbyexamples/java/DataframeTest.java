package com.sparkbyexamples.java;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
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
                .filter(flight.col("dist").gt(2700)) // dist > 2700
                .groupBy(flight.col("carrier"))
                .count()
                .orderBy(col("count"))
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("/tmp/flight.parquet");
        System.in.read();   // Now open http://localhost:4040/SQL/execution/?id=0
    }
}
