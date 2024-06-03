package com.sparkSession.pariRdd;

import com.sparkSession.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class UpperCase {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("UpperCase").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> citiesRDD = sc.textFile("in/cities.text");
        JavaRDD<String> citiesInUSA = citiesRDD.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));
        JavaPairRDD<String, String> citiesPairRdd = citiesInUSA.mapToPair(getAirportNameAndCountryNamePair());
        JavaPairRDD<String, String> upperCase = citiesPairRdd.mapValues(countryName -> countryName.toUpperCase());
        upperCase.saveAsTextFile("out/cities.text");

    }

    private static PairFunction<String, String, String> getAirportNameAndCountryNamePair(){
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1],
        line.split(Utils.COMMA_DELIMITER)[3]);
    }
}
