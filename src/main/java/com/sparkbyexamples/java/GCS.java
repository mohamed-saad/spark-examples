package com.sparkbyexamples.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class GCS {

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("GCS Test")
                .getOrCreate();
        spark.sparkContext().hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/workspace/keys/gcs-sys-1769.json");
        Dataset<Row> remote = spark.read().parquet("gs://sys-1769/tpch-parq-100/nation");
        remote.createOrReplaceTempView("nation");
        spark.sql("SELECT * FROM nation").show(100, false);
        System.in.read();   // Now open http://localhost:4040/SQL/execution/?id=0
    }

}
