package com.bigdatacompany.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkSqlFirst {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");

        StructType schema = new StructType().add("firstName", DataTypes.StringType)
                .add("lastName", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("gender", DataTypes.StringType)
                .add("country", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();
        Dataset<Row> rawDS = sparkSession.read().option("header",true).schema(schema).csv("C:\\Users\\HAZAL\\OneDrive\\Masaüstü\\person.csv");

        Dataset<Row> selDS = rawDS.select("firstName","email","country");

       // Dataset<Row> countryDS = selDS.filter(selDS.col("country").equalTo("France").or(selDS.col("country").equalTo("Brazil")));
       // Dataset<Row> chinaDS = selDS.filter(selDS.col("country").equalTo("China").and(selDS.col("age").(10).and.(selDS.col("email").contains("google")));

        Dataset<Row> frDS = selDS.filter("country = 'France' or country = 'Brazil' " );
        frDS.show();

    }
}
