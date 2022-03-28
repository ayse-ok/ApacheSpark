package com.example.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.spark_project.guava.collect.Iterables;

import com.example.spark.model.Person;

import scala.Tuple2;

public class MapTransformation {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");    	
        JavaSparkContext context = new JavaSparkContext("local","Map Transformation Spark");
        
        JavaRDD<String> rawdata = context.textFile("dataset/person.csv");
        
        JavaRDD<Person> loadPerson = rawdata.map(new Function<String, Person>() {				
			private static final long serialVersionUID = 1L;

			public Person call(String line) throws Exception {
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
        
        // pair func ilk değer girdi, 2 değer key, 3.değer value
        JavaPairRDD<String,Person> pairRdd = loadPerson.mapToPair(new PairFunction<Person, String, Person>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Person> call(Person person) throws Exception {
				return new Tuple2<String, Person>(person.getCountry(), person);
			}
		});
        
        JavaPairRDD<String,Iterable<Person>> groupData = pairRdd.groupByKey();
        
        groupData.foreach(new VoidFunction<Tuple2<String,Iterable<Person>>>() {		
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Iterable<Person>> data) throws Exception {
				System.out.println("Key : "+ data._1 + "- count: "+ Iterables.size(data._2));								
			}
		});
        
        
        //foreach ile ekrana yazdırma
//        loadPerson.foreach(new VoidFunction<Person>() {			
//			public void call(Person person) throws Exception {
//				System.out.println("Adı: "+ person.getFirst_name() + " Soyadı: "+ person.getLast_name());				
//			}
//		});
        
        
        
        // filter operation
//        JavaRDD<Person> personFromCounty = loadPerson.filter(new Function<Person, Boolean>() {
//			private static final long serialVersionUID = 1L;
//
//			public Boolean call(Person person) throws Exception {				
//				return person.getCountry().equals("Canada\"");
//			}        	
//		});
//        
//        System.out.println("Person count from Canada : " + personFromCounty.count());
//        personFromCounty.foreach(new VoidFunction<Person>() {
//			private static final long serialVersionUID = 1L;
//
//			public void call(Person p) throws Exception {
//				System.out.println(p.getFirst_name()+ "-" + p.getCountry());				
//			}        	
//		});
        
        
        
        
	}

}
