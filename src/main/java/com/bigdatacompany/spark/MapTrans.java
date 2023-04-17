package com.bigdatacompany.spark;

import com.bigdatacompany.spark.model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.FlatMapFunction;


import java.util.Arrays;
import java.util.Iterator;


public class MapTrans {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        JavaSparkContext javaSparkContext=new JavaSparkContext("local","Map Transformation Spark");

        JavaRDD<String> rawdata = javaSparkContext.textFile("C:\\Users\\HAZAL\\OneDrive\\Masaüstü\\person.csv");
        System.out.println(rawdata.count());
        JavaRDD<String> distData = rawdata.distinct();
        System.out.println(distData.count());

       /* JavaRDD<String> stringJavaRDD = rawdata.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        System.out.println(stringJavaRDD.count()); */

        /*
        JavaRDD<Person> loadPerson = rawdata.map(new Function<String, Person>() {
            public Person call(String line) throws Exception{
                String[] data = line.split(",");
                Person p = new Person();
                p.setFirst_name(data[0]);
                p.setLast_name(data[1]);
                p.setEmail(data[2]);
                p.setGender(data[3]);
                p.setCountry(data[4]);
                return p;
            }
        });
        */

        // Foreach ile ekrana yazdırma
        /*loadPerson.foreach(new VoidFunction<Person>() {

            public void call(Person person) throws Exception {
                System.out.println("Adı : "+ person.getFirst_name()+" Soyadı: "+person.getLast_name());
            }
        });
        */

        // Filter
        /*
        JavaRDD<Person> personFromCanada = loadPerson.filter(new Function<Person, Boolean>() {
            @Override
            public Boolean call(Person person) throws Exception {
                return person.getCountry().equals("Canada") && person.getGender().equals("Male");
            }
        });

        System.out.println("Person count :" + personFromCanada.count());
        personFromCanada.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person.getFirst_name()+" "+person.getLast_name()+" "+person.getCountry()+" "+person.getGender());
            }
        }); */



    }
}
