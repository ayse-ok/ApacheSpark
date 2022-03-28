package com.example.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkDataFrameApi {

	public static void main(String[] args) {
		//hadoop hatası vermemesi icin ekledik
    	System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");
    	
//    	createSchemaCsv();
    	
    	createSchemaJson();    	
	}
	
	private static void createSchemaCsv(){		
		StructType schema = new StructType().add("first_name", DataTypes.StringType)
	    		.add("last_name", DataTypes.StringType)
	    		.add("email", DataTypes.StringType)
	    		.add("gender", DataTypes.StringType)
	    		.add("country", DataTypes.StringType)
	    		.add("age", DataTypes.IntegerType);
	    	
	    	SparkSession sparkSession = SparkSession.builder().master("local").appName("Person Example").getOrCreate(); 
	    	Dataset<Row> rawDS = sparkSession.read().option("header",true).schema(schema).csv("dataset/person_yas.csv");
		//	rawDS.show();
	    //	rawDS.printSchema(); // kolon isimleri ve tiplerini, özelliklerini getirir
	    	
	    	
	    	filterCsv(rawDS);   // filter operation
	}
	
	private static void filterCsv(Dataset<Row> rawDS) {
		Dataset<Row> selectDS = rawDS.select("first_name","country"); 	// kolon seçebiliyoruz
		
	 	Dataset<Row> chinaDS = rawDS.filter(selectDS.col("country").equalTo("China"));
	    //	countryChinaDS.show();
	    	
	    	Dataset<Row> ageGt70DS = rawDS.filter("age > 70 ").sort("age");
	   // 	ageGt70DS.show();
	    	
	    	Dataset<Row> chinaGt50DS = rawDS.filter(rawDS.col("country").equalTo("China").and(rawDS.col("age").gt(50)).and(rawDS.col("gender").equalTo("Female")));
	    //	chinaGt50DS.show();
	    	
	    	Dataset<Row> groupCountDS = selectDS.groupBy("country").count();
	    //	groupCountDS.show();
	}
	
	
	
	private static void createSchemaJson(){
		String filePath = "dataset/productData.json";
		StructType schema = new StructType().add("first_name", DataTypes.StringType)
	    		.add("last_name", DataTypes.StringType)
	    		.add("email", DataTypes.StringType)
	    		.add("gender", DataTypes.StringType)
	    		.add("country", DataTypes.StringType)
	    		.add("price", DataTypes.DoubleType)
	    		.add("product", DataTypes.StringType);
		
		SparkSession sparkSession = SparkSession.builder().master("local").appName("Person Example").getOrCreate(); 
		Dataset<Row> rawDS = sparkSession.read().schema(schema).option("multiline", true).json(filePath);
	//	rawDS.show();
	//	rawDS.printSchema();
		
		filterJson(rawDS);		// filter operation
	}
	
	
	private static void filterJson(Dataset<Row> rawDS) {
//		Dataset<Row> countPriceDS = rawDS.groupBy("country","product").sum("price");
//		countPriceDS.show();
		
//		Dataset<Row> countDS = rawDS.groupBy("country","product").count();
//		countDS.sort(functions.desc("count")).show();
		
		Dataset<Row> avgPriceDS = rawDS.groupBy("country","product").avg("price");
		avgPriceDS.show();
		
	}
}
