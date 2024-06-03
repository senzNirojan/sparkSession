package com.sparkSession.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogs {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("UnionLogs").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> firstLogs = sc.textFile("in/nasa_log_01.tsv");
        JavaRDD<String> secondLogs = sc.textFile("in/nasa_log_02.tsv");

        JavaRDD<String> aggregatedLogs = firstLogs.union(secondLogs);
        JavaRDD<String> cleanLogs = aggregatedLogs.filter(line -> isNotHeader(line));
        cleanLogs.coalesce(1).saveAsTextFile("out/aggregated_logs.csv");

    }

    private static boolean isNotHeader(String line){
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}
