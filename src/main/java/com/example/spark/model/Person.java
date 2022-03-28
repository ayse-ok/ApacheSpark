package com.example.spark.model;

import java.io.Serializable;

import lombok.Data;

@Data
public class Person implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private String first_name;
	private String last_name;
	private String email;
	private String gender;
	private String country;
	private Integer age;
	private Double price;
	private String product;
}
