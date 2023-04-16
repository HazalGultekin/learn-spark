package com.bigdatacompany.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FirstExam {
    private static Object JavaRDD;

    public static void main(String[] args) {
        JavaSparkContext javaSparkContext=new JavaSparkContext("local","First Exam Spark");
        JavaRDD<String> firstData = javaSparkContext.textFile("C:\\Users\\HAZAL\\OneDrive\\Masaüstü\\firstdata.txt");
        System.out.println(firstData.first());

    }
}
