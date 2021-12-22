package com.sparkbyexamples.java;

import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Deltalake {

    private static final int RUNS = 1000;

    private static long benchmark(SparkSession spark, String table) {
        long start = System.currentTimeMillis();
        for (int i=0; i<RUNS; i++)
            spark.sql("SELECT max(SALARY) FROM " + table + " WHERE EMPLOYEE_ID > " + (int)(Math.random() * RUNS)).collect();
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static void load(SparkSession spark, Supplier<String> title, Consumer<String> loader) {
        long start = System.currentTimeMillis();
        String table = "Emp";
        loader.accept(table);
        long query = benchmark(spark, table);
        long end = System.currentTimeMillis();
        System.out.println("\n" + title.get() + "\tend-2-end: " + (end - start));
        System.out.println(title.get() + "\t" + RUNS + " queries: " + query);
    }

    public static void main(String args[]) throws IOException {
        // disable loggers
        org.apache.log4j.Logger.getLogger("org").setLevel(Level.FATAL);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.FATAL);

        // start session
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Deltalake")
//                .config("spark.databricks.delta.snapshotPartitions","1")
//                .config("spark.sql.sources.parallelPartitionDiscovery.parallelism","1")
//                .config("delta.log.cacheSize","3")
//                .config("spark.ui.enabled","false")
//                .config("spark.ui.showConsoleProgress","false")
                .getOrCreate();

        // benchmark
        load(spark, () -> "Parquet",    table -> spark.read().format("parquet").load(String.format("src/main/resources/delta/c/%s/%s/part-000000", "1638792353212", "1638792353212")).createOrReplaceTempView(table));
        load(spark, () -> "Delta File", table -> spark.read().format("delta").load("src/main/resources/delta/c").createOrReplaceTempView(table));
        load(spark, () -> "Delta Ext",  table -> {
            spark.sql("CREATE TABLE " + table + " USING DELTA LOCATION '/workspace/git/spark-examples/src/main/resources/delta/c'");
            spark.sql("ANALYZE TABLE " + table + " COMPUTE STATISTICS NOSCAN");
        });

        // wait
        System.out.println("\n\nPress any key to continue");
        System.in.read();
    }

}
