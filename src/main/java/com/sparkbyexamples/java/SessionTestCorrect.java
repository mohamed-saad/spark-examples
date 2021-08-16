package com.sparkbyexamples.java;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SessionTestCorrect {

    static class Info implements Serializable {
        int countOfNames;
        int lengthOfNames;

        Info(int countOfNames , int lengthOfNames) {
            this.countOfNames = countOfNames;
            this.lengthOfNames = lengthOfNames;
        }
    }

    static class Result implements Serializable {
        char letter;
        float average;

        Result(char letter, float average) {
            this.letter = letter;
            this.average = average;
        }
    }

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[6]")
                .appName("Names")
                .getOrCreate();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<String> data = Arrays.asList("Ahmed", "Ali", "Amir", "Basem", "Badr", "Bahaa", "Emad", "Eman",
                "Essam", "Fawzy", "Farouk", "Fayrouz", "Gamal", "Galal", "Ghada", "Heba", "Hadeer", "Halah");
        JavaRDD<String> rdd = sparkContext.parallelize(data, 13);

        rdd.mapToPair(name -> new Tuple2<>(name.charAt(name.length()-1), new Info( 1, name.length() )))
                .reduceByKey((a, b) -> new Info ( a.countOfNames + b.countOfNames, a.lengthOfNames + b.lengthOfNames ))
                .map(tuple -> new Result(tuple._1, (float)tuple._2.lengthOfNames/tuple._2.countOfNames))
                .foreach(result -> System.out.println(result.letter + " = " + result.average));

        System.in.read();   // Now open http://localhost:4040/
    }
}
