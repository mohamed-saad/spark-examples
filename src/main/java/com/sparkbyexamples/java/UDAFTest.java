package com.sparkbyexamples.java;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.functions;

import java.io.IOException;

public class UDAFTest {

    public static class DelayStatusUDF extends Aggregator<Double, DelayStatus, String> {

        @Override
        public DelayStatus zero() {
            return new DelayStatus();
        }

        @Override
        public DelayStatus reduce(DelayStatus b, Double a) {
            if (a < 20) b.incCountInTime();
            else if (a < 40) b.incCountAroundTime();
            else if (a < 80) b.incCountDelayed();
            else b.incCountProblem();
            return b;
        }

        @Override
        public DelayStatus merge(DelayStatus b1, DelayStatus b2) {
            DelayStatus sum = new DelayStatus();
            sum.setCountInTime(b1.getCountInTime() + b2.getCountInTime());
            sum.setCountAroundTime(b1.getCountAroundTime() + b2.getCountAroundTime());
            sum.setCountDelayed(b1.getCountDelayed() + b2.getCountDelayed());
            sum.setCountProblem(b1.getCountProblem() + b2.getCountProblem());
            return sum;
        }

        @Override
        public String finish(DelayStatus reduction) {
            int total = reduction.getCountInTime() + reduction.getCountAroundTime() + reduction.getCountDelayed() + reduction.getCountProblem();
            return String.format("%d%% In time, %d%% Around, %d%% Delayed, %d%% Problems",
                    reduction.getCountInTime() * 100 / total,
                    reduction.getCountAroundTime() * 100 / total,
                    reduction.getCountDelayed() * 100 / total,
                    reduction.getCountProblem() * 100 / total);
        }

        @Override
        public Encoder<DelayStatus> bufferEncoder() {
            return Encoders.bean(DelayStatus.class);
        }

        @Override
        public Encoder<String> outputEncoder() {
            return Encoders.STRING();
        }
    }

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Flight")
                .getOrCreate();
        spark.sqlContext().udf().register("DelayStatus", functions.udaf(new DelayStatusUDF(), Encoders.DOUBLE()));
        Dataset<Row> flight = spark.read().json("src/main/resources/flight.json").repartition(8);
        flight.createOrReplaceTempView("US_FLIGHT");
        spark.sql("SELECT carrier, DelayStatus(depdelay), DelayStatus(arrdelay) FROM US_FLIGHT GROUP BY carrier").show(false);
    }
}
