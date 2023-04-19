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
        Dataset<Row> rawDS = sparkSession.read().schema(schema).option("multiline",true).json("C:\\Users\\HAZAL\\OneDrive\\Masaüstü\\product.json");
        Dataset<Row> counPriceDS = rawDS.groupBy("country".avg("price"));


    }
}
