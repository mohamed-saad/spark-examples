package com.sparkbyexamples.java;

import org.apache.log4j.Level;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;

public class CacheCheckpointTest {

    public static void main(String args[]) throws IOException {
        // disable loggers
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.FATAL);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.FATAL);

        // create spark session
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .getOrCreate();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        sparkContext.setCheckpointDir("/tmp/spark-checkpoint");
        Dataset<Row> flight = spark.read().json("src/main/resources/flight.json").repartition(8);
//        Dataset<Row> longFlights = flight.filter(flight.col("dist").gt(2700));
//        Dataset<Row> longFlights = flight.filter(flight.col("dist").gt(2700)).checkpoint(); // enable checkpoint
//        Dataset<Row> longFlights = flight.filter(flight.col("dist").gt(2700)).cache(); // enable cache
        Dataset<Row> longFlights = flight.filter(flight.col("dist").gt(2700)).persist(StorageLevel.DISK_ONLY());

        // get counts of long flight
        long count = longFlights.count();
        System.out.println("Count of Long Flights = " + count);

        // get counts of long flights that has departure delays
        long delayedCount = longFlights.filter(flight.col("depdelay").gt(120)).count();   // depdelay > 120
        System.out.println("Count of Long Flights with more than 2 hours departure delay = " + delayedCount);

        // get counts of long flights grouped by source airport
        System.out.println("Count of Long Flights by Source");
        longFlights.groupBy(flight.col("src")).count().orderBy(col("count")).show();

        // get counts of long flights grouped by carrier
        System.out.println("Count of Long Flights by Carrier");
        longFlights.groupBy(flight.col("carrier")).count().orderBy(col("count")).show();

        System.in.read();   // Now open http://localhost:4040/SQL/execution/?id=0
    }
}
