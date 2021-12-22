package com.sparkbyexamples.java;

import org.apache.log4j.Level;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class Deltalake_TPC {

    private static final String LINE = "-------------------------------------------------------------------------------------";

    private static void createTables(SparkSession spark, String format, String path) {
        List<String> tables = Arrays.stream(new File(path).list()).collect(Collectors.toList());
        System.out.println("Create " + tables.size() + " tables using " + format + " format");
        for (String table: tables) {
            spark.read().format(format).load(path + "/" + table).createOrReplaceTempView(table);
        }
    }

    private static long executeQuery(SparkSession spark, String name, String query) {
        System.out.println("Executing " + name);
        spark.sparkContext().setJobDescription(name);
        long start = System.currentTimeMillis();
        int collected = 0;

        Iterator<Row> data = spark.sql(query).toLocalIterator();
        while (true) {
            try {
                data.next();
                collected++;
            } catch (NoSuchElementException e) {
                break;
            }
        }
        long end = System.currentTimeMillis();
        long time = end - start;
        System.out.println("Executed " + name + " in " + time / 1000 + " seconds returned " + collected + " rows");
        return time;
    }

    private static void queryTPC(SparkSession spark, String queryDir, int runs) throws IOException {
        System.out.println("Running queries for " + runs + " runs");
        Map<String, List<Long>> executionTime = new TreeMap<>();
        File[] files = new File(queryDir).listFiles();
        Arrays.sort(files);
        for (int r=0; r<runs; r++) {
            System.out.println(LINE);
            System.out.println("Run# " + (r+1) + "/" + runs);
            for (File f : files) {
                String query = new String(Files.readAllBytes(f.toPath()));
                executionTime.computeIfAbsent(f.getName(), k -> new LinkedList<>()).add(executeQuery(spark, f.getName(), query));
            }
        }

        System.out.println(LINE);
        long totalTime = 0;
        for (Map.Entry<String, List<Long>> query : executionTime.entrySet()) {
            System.out.print(query.getKey());
            for (Long t: query.getValue()) {
                totalTime += t;
                System.out.print("\t" + t);
            }
            System.out.println();
        }
        System.out.println(LINE);
        System.out.println("Total Time\t" + totalTime);
    }

    private static void queryAggregate(SparkSession spark, String query, int runs) throws IOException {
        long start = System.currentTimeMillis();
        for (int i=0; i<runs; i++)
            spark.sql(String.format(query, (int)(Math.random() * runs))).collect();
        long end = System.currentTimeMillis();
        System.out.println("\n");
        System.out.println(query);
        System.out.println(LINE);
        System.out.println("Total Time\t" + (end - start));
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
                .config("spark.ui.enabled","false")
                .config("spark.ui.showConsoleProgress","false")
                .getOrCreate();

        boolean useDelta = true;
        if (useDelta)
            createTables(spark, "delta", "/home/msaad/workspace/data/tpch-50g-delta");
        else
            createTables(spark, "parquet", "/home/msaad/workspace/data/tpch-50g-parq");
        queryTPC(spark, "/home/msaad/workspace/git/spark-examples/src/main/resources/tpch-queries-s100", 3);
        queryAggregate(spark, "SELECT max(l_quantity) FROM lineitem WHERE l_orderkey > %d", 100);
        queryAggregate(spark, "SELECT * FROM lineitem WHERE l_orderkey > %d LIMIT 1", 1000);
        queryAggregate(spark, "SELECT min(length(n_comment)) FROM nation WHERE n_nationkey > %d",1000);
    }
}
