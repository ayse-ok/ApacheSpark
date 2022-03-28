package com.example.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadFile {
    public static void main( String[] args ){
    	//hadoop hatası vermemesi icin ekledik
    	System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");
    	
        JavaSparkContext context = new JavaSparkContext("local","First Example Spark");
        
     // veriyi analizden önce Rdd ye yüklüyoruz        
  //    textLoad(context);
        
        codeDataLoad(context); 
    }
    
    
    // local text data - verileri okuyoruz yani dışardan okuduğumuz veri setini rdd ye çeviriyoruz
    public static void textLoad(JavaSparkContext context) {    	
        JavaRDD<String> firstdata = context.textFile("dataset/firstData.txt");
        System.out.println("Toplam veri sayısı : " + firstdata.count());
        System.out.println("ilk veri : " + firstdata.first());
    }
    
    
    // local code data - // dışardan değil de kod içindeki dataları liste çeviriyoruz yani kod içinde rdd oluşturuyoruz
    public static void codeDataLoad(JavaSparkContext context) {
    	List<String> data = Arrays.asList("data 1","data 2","data 3","data 4","data 5");
    	JavaRDD<String> firstdata = context.parallelize(data);		
    	System.out.println("Toplam veri sayısı : " + firstdata.count());
    	System.out.println("ilk veri : " + firstdata.first());
    }
}
