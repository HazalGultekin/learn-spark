package com.bigdatacompany.spark;

import com.bigdatacompany.spark.model.Person;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;


import java.util.Arrays;
import java.util.Iterator;


public class MapTrans {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        JavaSparkContext javaSparkContext=new JavaSparkContext("local","Map Transformation Spark");

        // Distinct Kullanımı
       /* JavaRDD<String> rawdata = javaSparkContext.textFile("C:\\Users\\HAZAL\\OneDrive\\Masaüstü\\person.csv");
        System.out.println(rawdata.count());
        JavaRDD<String> distData = rawdata.distinct();
        System.out.println(distData.count());*/

       /* JavaRDD<String> stringJavaRDD = rawdata.flatMap(new FlatMapFunction<String, String>() {

       JavaRDD<String> rawdata = javaSparkContext.textFile("C:\\Users\\HAZAL\\OneDrive\\Masaüstü\\person.csv");

        // FlatMap Kullanımı
        /* JavaRDD<String> stringJavaRDD = rawdata.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        System.out.println(stringJavaRDD.count()); */


       // Map Kullanımı
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

        JavaPairRDD<String, String> pairRdd = loadPerson.mapToPair(new PairFunction<Person, String, String>() {
            @Override
            public Tuple2<String, String> call(Person person) throws Exception {
                return new Tuple2<String, String>(person.getEmail(), person.getCountry());
            }
        });

        pairRdd.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> data) throws Exception {
                System.out.println("Key : "+ data._1+" -- Value : "+data._2);
            }
        });


        // Foreach ile ekrana yazdırma
        /*loadPerson.foreach(new VoidFunction<Person>() {

            public void call(Person person) throws Exception {
                System.out.println("Adı : "+ person.getFirst_name()+" Soyadı: "+person.getLast_name());
            }
        });*/



        // Filter Kullanımı
        /*JavaRDD<Person> personFromCanada = loadPerson.filter(new Function<Person, Boolean>() {
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
