package com.bigdatacompany.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class ActiveApp {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        JavaSparkContext javaSparkContext=new JavaSparkContext("local","Map Transformation Spark");
        JavaRDD<String> rawdata = javaSparkContext.textFile("C:\\Users\\HAZAL\\OneDrive\\Masaüstü\\person.csv");

        rawdata.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
