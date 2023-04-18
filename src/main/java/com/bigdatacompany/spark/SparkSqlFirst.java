package com.bigdatacompany.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSqlFirst {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();
        Dataset<Row> rawDS = sparkSession.read().option("header",true).csv("C:\\Users\\HAZAL\\OneDrive\\Masaüstü\\person.csv");

        Dataset<Row> wcDS = rawDS.withColumn("firstNameTest", rawDS.col("firstName"));
        wcDS.show();
    }
}
