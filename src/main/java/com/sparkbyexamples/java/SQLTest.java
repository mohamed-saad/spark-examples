package com.sparkbyexamples.java;

import org.apache.spark.sql.*;
import java.io.IOException;

public class SQLTest {

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .getOrCreate();
        Dataset<Row> flight = spark.read().json("src/main/resources/flight.json").repartition(8);
        flight.createOrReplaceTempView("US_FLIGHT");
        spark.sql("SELECT carrier, COUNT(*) as flights FROM US_FLIGHT WHERE dist > 2700 GROUP BY carrier ORDER BY flights")
            .write()
            .mode(SaveMode.Overwrite)
            .parquet("/tmp/flight.parquet");
//        spark.sql("SELECT COUNT(*) FROM US_FLIGHT").show(); // 282628
//        spark.sql("SELECT src, dst, depdelay FROM US_FLIGHT WHERE depdelay > 40 AND carrier = 'AA'").show(5); // narrow transformation
//        spark.sql("SELECT carrier, COUNT(*) FROM US_FLIGHT GROUP BY carrier").show(5); // wide transformation
        System.in.read();   // Now open http://localhost:4040/SQL/execution/?id=0
    }
}
