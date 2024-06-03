package com.sparkSession.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Actions {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("Actions").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "kafka", "hadoop", "jenkins");
        JavaRDD<String> wordsRDD = sc.parallelize(inputWords);

        //first action name is collect
        List<String> words = wordsRDD.collect();

        for (String word : words) {
            System.out.println(word);
        }

        //Second action is count
        long count = wordsRDD.count();
        System.out.println(count);

        //third action is countByValue
        Map<String, Long> countByValue = wordsRDD.countByValue();
        countByValue.forEach((key,value) -> System.out.println((key + " : " + value)));

        //Fourth action is take
        List<String> firstThree = wordsRDD.take(3);
        System.out.println("First three elements: " + firstThree);

        //Fifth action is reduce
        List<Integer> inputIntegers = Arrays.asList(3,2,5,8,1);
        JavaRDD<Integer> integerJavaRDD = sc.parallelize(inputIntegers);

        Integer product = integerJavaRDD.reduce((x,y) -> x*y);
        System.out.println("Product is: " + product);
    }
}
