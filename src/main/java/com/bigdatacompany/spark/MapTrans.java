package com.bigdatacompany.spark;

import com.bigdatacompany.spark.model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;




public class MapTrans {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        JavaSparkContext javaSparkContext=new JavaSparkContext("local","Map Transformation Spark");

        JavaRDD<String> rawdata = javaSparkContext.textFile("C:\\Users\\HAZAL\\OneDrive\\Masaüstü\\person.csv");

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

        loadPerson.foreach(new VoidFunction<Person>() {

            public void call(Person person) throws Exception {
                System.out.println("Adı : "+ person.getFirst_name()+" Soyadı: "+person.getLast_name());
            }
        });



    }
}
