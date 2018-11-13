package com.magarkamal.spark.spark_java_example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
       
    	
    	SparkSession session = SparkSession.builder()
    			.appName("SparkJavaExample")
    			.master("local[3]")//number of the thread you want a run
    			.getOrCreate();
    	
    	JavaSparkContext context = new JavaSparkContext(session.sparkContext());
    	List<Integer> integers = Arrays.asList(1,3,5,2,44,32,67,55,20,16,35,78,69,72);
    	
    	JavaRDD<Integer> javaRDD = context.parallelize(integers,3);//number of executer
    	
    	javaRDD.foreach((VoidFunction<Integer>) integer -> System.out.println("Java RDD:"+integer));
    	
    	try {
			Thread.sleep(100000000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	context.close();
    	
    }
}
