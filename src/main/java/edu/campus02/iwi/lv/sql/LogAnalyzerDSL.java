package edu.campus02.iwi.lv.sql;

import static org.apache.spark.sql.functions.*;

import java.util.List;
import java.util.Objects;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import spark.exercise.env.WinConfig;

public class LogAnalyzerDSL {

	public static void main(String[] args) {

		WinConfig.setupEnv();
		
		if (args.length != 1) {
			System.err.println("usage: program <path_to_apache_log_file>");
			System.exit(-1);
		}

		//TODO 1: create spark config & session instead of context
		
		
		//TODO 2: read in the access log file with string encoder
		//and map the elements with the bean encoder to ApacheAccessLog objects
		//we also filter them in order to keep only non-null entries
		
		
		//TODO 3: inspect the schema and a few sample records
		
		//TODO 4: simple grouping by HTTP method
		
		//TODO 5: calculate and show the top 25 requested resources (=endpoints)
		
		//TODO 6: collect all hosts (=ipAddress) that have accessed
		//the server more than 250 times into  list and it to the console
		
		//TODO 7: apply the aggregate functions avg, min, max, sum
		//based on contentSize column of requested resources
		//and show the results
		
		
	}
}