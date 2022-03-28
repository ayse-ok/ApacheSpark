package com.example.spark;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkSqlApi implements Serializable{
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		createSchemaJson();
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
		
		rawDS.createOrReplaceTempView("personView");		// session bazlı view oluşturur
//		rawDS.createOrReplaceGlobalTempView("personView");		// cluster üzerinde view oluşturur başka sql job lar üzerinden de erişilebilir
		
		Dataset<Row> sqlData = sparkSession.sql("select * from personView where country ='Sweden'");  		// filter sql ile yapılabiliyor
		sqlData.show();
	}
}
