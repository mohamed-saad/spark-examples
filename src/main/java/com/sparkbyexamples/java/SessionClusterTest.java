package com.sparkbyexamples.java;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SessionClusterTest {

    public static void main(String args[]) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://dr-mohamed-saad:7077")
                .appName("Names")
                .config("spark.executor.instances","3")
                .config("spark.executor.cores", "4")
                .config("spark.executor.memory", "1g")
                .getOrCreate();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<String> data = Arrays.asList("Ahmed", "Ali", "Amir", "Basem", "Badr", "Bahaa", "Emad", "Eman",
                "Essam", "Fawzy", "Farouk", "Fayrouz", "Gamal", "Galal", "Ghada", "Heba", "Hadeer", "Halah");
        JavaRDD<String> rdd = sparkContext.parallelize(data, 13);
        rdd .mapToPair(name -> new Tuple2<>(name.charAt(name.length()-1), name.length()))
            .reduceByKey((a, b) -> a+b)
            .foreach(e -> System.out.println(e._1 + " = " + e._2));

        System.in.read();   // Now open http://localhost:4040/
    }
}
