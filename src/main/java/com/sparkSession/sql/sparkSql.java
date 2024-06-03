package com.sparkSession.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class sparkSql {

    private static final String SALARY_MIDPOINT = "salary_midpoint";
    private static final String AGE_MIDPOINT = "age_midpoint";

    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder().appName("SparkSQL").master("local[2]").getOrCreate();
        DataFrameReader dataFrameReader = session.read();

        Dataset<Row> responses = dataFrameReader.option("header", "true").csv("in/survey-responses.csv");
        System.out.println("=== Print out the Schema ===");
        responses.printSchema();

        System.out.println("=== Print some data form the table ===");
        responses.show(10);

        System.out.println("=== Print some columns only ===");
        responses.select(col("country"), col("self_identification")).show();

        System.out.println("=== Print records from a specific country");
        responses.filter((col("country").equalTo("Afghanistan"))).show();

        System.out.println("=== Print some records by using groups ===");
        RelationalGroupedDataset groupedDataset = responses.groupBy(col("age_range"));
        groupedDataset.count().show();

        Dataset<Row> castedResponse = responses.withColumn(SALARY_MIDPOINT, col(SALARY_MIDPOINT).cast("integer")).withColumn(AGE_MIDPOINT, col(AGE_MIDPOINT).cast("integer"));
        System.out.println("=== Print the average mid age less than 20 ===");
        castedResponse.filter(col(AGE_MIDPOINT).$less(20)).show();

        System.out.println("=== Print some results in descending order for salary mid point ===");
        castedResponse.orderBy(col(SALARY_MIDPOINT).desc()).show();

        System.out.println("=== Group by salary bucket ===");
        Dataset<Row> responseWithSalaryBucket = castedResponse.withColumn("salary_midpoint_bucket", col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000));
        responseWithSalaryBucket.groupBy("salary_midpoint_bucket").count().orderBy(col("salary_midpoint_bucket")).show();

        session.stop();

    }
}
