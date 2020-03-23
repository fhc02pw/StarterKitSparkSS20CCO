package edu.campus02.iwi.lv.warmup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import spark.exercise.env.WinConfig;

public class BigramAnalyzer {

	public static void main(String[] args) {
		
		WinConfig.setupEnv();
		
		if(args.length != 1) {
			System.err.println("usage: program <input_dir>");
			System.exit(-1);
		}
		
		SparkConf cnf = new SparkConf().setMaster("local[1]")
				.setAppName(BigramAnalyzer.class.getName());

		JavaSparkContext spark = new JavaSparkContext(cnf);
			     
		JavaRDD<String> lines = spark.textFile(args[0]+"/*.txt");
									
        JavaPairRDD<String, Integer> bigramCounts = 
	        		lines.flatMap(line -> {
	        			//TODO 1: toLowerCase, character removal and split by whitespace
						line = line.toLowerCase();
						line = line.replaceAll("[^a-z\\s]", "");
						String[] splitted = line.split(" ");

						//TODO 2: emit word pairs as space separated strings
						// example: ["apache","spark","is","nice"]
						// result: ["apache spark","spark is","is nice"]
						List<String> pairs = new ArrayList<String>();
						for(int i = 0; i < splitted.length - 1; i++)
						{
							String paired = splitted[i] + " " + splitted[i + 1];
							pairs.add(paired);
						}

						//TODO 3: change the return below to return
						// an iterator of the resulting list from TODO 2 above
						//return Arrays.asList(line.split("")).iterator();
						return pairs.iterator();
					})
	        		.mapToPair(word -> new Tuple2<>(word,1))
	        			.reduceByKey((c1,c2) -> c1+c2);


	    
        //TODO 4: swap tuples, sort by counts desc and take first N e.g. 3
        // to write the most frequent bigram to console -> change the below line

		JavaPairRDD<Integer, String> swapped = bigramCounts.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) item -> item.swap());

        List<Tuple2<Integer,String>> sorted = swapped.sortByKey(false).collect();

        //sorted.forEach(System.out::println);

        for(int i = 0; i < 30; i++)
		{
			System.out.println("Häufigste: " + sorted.get(i));
		}
        
		spark.close();

	}

}
