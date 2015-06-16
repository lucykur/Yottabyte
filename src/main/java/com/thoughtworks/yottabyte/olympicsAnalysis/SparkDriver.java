package com.thoughtworks.yottabyte.olympicsAnalysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Collections;
import java.util.Comparator;

public class SparkDriver {
    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String path = args[2];

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile(path);

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<>(words[1], Integer.parseInt(words[7]));
            }
        });

        final JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer acc, Integer totalByCountry) throws Exception {
                return acc + totalByCountry;
            }
        });


        JavaPairRDD<Integer, String> swapped = stringIntegerJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });

        Tuple2<Integer, String> first = swapped.sortByKey(false).first();

//        stringIntegerJavaPairRDD.saveAsTextFile(destination);

        System.out.println(String.format("%s:%s", first._1, first._2));
    }
}
