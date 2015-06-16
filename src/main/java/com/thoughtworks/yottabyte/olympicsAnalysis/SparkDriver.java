package com.thoughtworks.yottabyte.olympicsAnalysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkDriver {
    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String path = args[2];

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile(path);

        JavaPairRDD<Tuple2<String, Integer>, Integer> pairs = lines.mapToPair(new PairFunction<String, Tuple2<String, Integer>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, Integer>, Integer> call(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<>(new Tuple2<String, Integer>(words[1], Integer.parseInt(words[2])), Integer.parseInt(words[7]));
            }
        });

        JavaPairRDD<Tuple2<String, Integer>, Integer> totals = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer acc, Integer medals) throws Exception {
                return acc + medals;
            }
        });

        JavaPairRDD<Integer, Tuple2<String, Integer>> swapped = totals.mapToPair(new PairFunction<Tuple2<Tuple2<String, Integer>, Integer>, Integer, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<Integer, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, Integer>, Integer> tuple2IntegerTuple2) throws Exception {
                return new Tuple2<Integer, Tuple2<String, Integer>>(tuple2IntegerTuple2._2, tuple2IntegerTuple2._1);
            }
        });

        Tuple2<Integer, Tuple2<String, Integer>> first = swapped.sortByKey(false).first();

//        stringIntegerJavaPairRDD.saveAsTextFile(destination);

        System.out.println(String.format("%s:%s, %s", first._1, first._2._1, first._2._2));
    }
}
