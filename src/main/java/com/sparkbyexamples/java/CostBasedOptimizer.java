package com.sparkbyexamples.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class CostBasedOptimizer {

    // http://www.openkb.info/2021/02/spark-tuning-understand-cost-based.html
    // https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-cost-based-optimization.html
    // https://medium.com/@mishra.writeto/spark-cost-based-optimizer-62a120b28367
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .config("spark.sql.cbo.enabled","true")                         // Enables CBO for estimation of plan statistics when set true.
                .config("spark.sql.cbo.planStats.enabled", "true")              // The logical plan will fetch row counts and column statistics from catalog.
                .config("spark.sql.cbo.starSchemaDetection", "true")            // It enables join reordering based on star schema detection.
                .config("spark.sql.cbo.joinReorder.dp.star.filter", "true")     // Applies star-join filter heuristics to cost based join enumeration.
                .config("spark.sql.cbo.joinReorder.dp.threshold", "12")         // The maximum number of joined nodes allowed in the dynamic programming algorithm.
                .config("spark.sql.cbo.joinReorder.enabled","true")             // Enables join reorder in CBO.
                .config("spark.sql.statistics.histogram.enabled","true")        // Generates histograms when computing column statistics if enabled.
                .config("spark.sql.statistics.histogram.numBins", "254")        // The default number of histogram buckets
                .config("spark.sql.statistics.fallBackToHdfs", "true")          // Fall back to HDFS if the table statistics are not available from table metadata
                .config("spark.sql.adaptive.enabled", "true")                   // enable adaptive query execution, which re-optimizes the query plan in the middle of query execution, based on accurate runtime statistics.
                .config("spark.sql.adaptive.localShuffleReader.enabled","true") // Spark tries to use local shuffle reader to read the shuffle data when the shuffle partitioning is not needed
                .config("spark.sql.adaptive.skewJoin.enabled", "true")          // Spark dynamically handles skew in sort-merge join by splitting (and replicating if needed) skewed partitions
                .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor","5") // A partition is considered as skewed if its size is larger than this factor multiplying the median partition size and also larger than 'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes'
                .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes","256MB") // A partition is considered as skewed if its size in bytes is larger than this threshold and also larger than it

                .getOrCreate();

        // read JSON and write Parquet
        Dataset<Row> flight = spark.read().json("src/main/resources/flight.json").repartition(8);
        flight.write().mode(SaveMode.Overwrite).parquet("src/main/resources/flight.parquet");

        // create table, optimize it and quer it
        spark.sql("CREATE TABLE US_FLIGHT USING PARQUET LOCATION '/workspace/git/spark-examples/src/main/resources/flight.parquet'");
        spark.sql("ANALYZE TABLE US_FLIGHT COMPUTE STATISTICS FOR COLUMNS src, dst, depdelay, carrier");
        // spark.sql("ANALYZE TABLE US_FLIGHT COMPUTE STATISTICS NOSCAN");
        spark.sql("SELECT COUNT(*) FROM US_FLIGHT").show(); // 282628
        spark.sql("SELECT src, dst, depdelay FROM US_FLIGHT WHERE depdelay > 40 AND carrier = 'AA'").show(100);
        spark.sql("SELECT min(depdelay) FROM US_FLIGHT").show(100);
        spark.sql("DESC EXTENDED US_FLIGHT").show(100, false);
        spark.sql("DESC EXTENDED US_FLIGHT src").show(100, false);
        spark.sql("DESC EXTENDED US_FLIGHT dst").show(100, false);
        spark.sql("DESC EXTENDED US_FLIGHT depdelay").show(100, false);
        spark.sql("DESC EXTENDED US_FLIGHT carrier").show(100, false);

        // create temp view and query it
        spark.read().parquet("/workspace/git/spark-examples/src/main/resources/flight.parquet").createOrReplaceTempView("US_FLIGHT_VW");
        spark.sql("SELECT src, dst, depdelay FROM US_FLIGHT_VW WHERE depdelay > 40 AND carrier = 'AA'").show();

        // wait
        System.in.read();   // Now open http://localhost:4040/SQL/execution/?id=0
    }
}
