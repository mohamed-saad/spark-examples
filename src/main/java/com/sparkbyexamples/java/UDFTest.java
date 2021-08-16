package com.sparkbyexamples.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;

public class UDFTest {

    public static class DelaySeverityUDF implements UDF1<Double, String> {
        @Override
        public String call(Double delay) throws Exception {
            if (delay < 20) return "In Time";
            if (delay < 40) return "Around Time";
            if (delay < 80) return "Delayed";
            return "Problem";
        }
    }

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .getOrCreate();
        spark.sqlContext().udf().register("Delay", new DelaySeverityUDF(), DataTypes.StringType);
        Dataset<Row> flight = spark.read().json("src/main/resources/flight.json").repartition(8);
        flight.createOrReplaceTempView("US_FLIGHT");
        spark.sql("SELECT id, Delay(depdelay), Delay(arrdelay) FROM US_FLIGHT").show(false);
        spark.sql("SELECT Delay(depdelay) as status, count(*) FROM US_FLIGHT GROUP BY status").show(false);
    }
}
