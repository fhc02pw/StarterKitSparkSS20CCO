package edu.campus02.iwi.lv.warmup;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import spark.exercise.env.WinConfig;

public class SimpleLineFilter {

	public static void main(String[] args) {
		
		WinConfig.setupEnv();
		
		if(args.length != 2) {
			System.err.println("usage: program <input_dir> <regex_pattern>");
			System.exit(-1);
		}
		
		//TODO 1: creation of spark config and spark context
		SparkConf cnf = new SparkConf().setMaster("local").setAppName(SimpleLineFilter.class.getName());
		JavaSparkContext spark = new JavaSparkContext(cnf);

		//TODO 2: read the txt input file(s) from the specified dir
		JavaRDD<String> textFile = spark.textFile(args[0]);

		//TODO 3: filter for lines matching the specified regexp pattern
		JavaRDD<String> filteredLines = textFile.filter(l -> l.matches(args[1]));

		//TODO 4: for demo purposes collect the result to a java List
		//and print to console each entry and the total number of entries found
		List<String> result = filteredLines.collect();
		for(String r : result)
		{
			System.out.println(r);
		}
		

	}

}
