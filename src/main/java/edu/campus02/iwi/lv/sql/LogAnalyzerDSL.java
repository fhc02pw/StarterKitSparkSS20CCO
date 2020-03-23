package edu.campus02.iwi.lv.sql;

import static org.apache.spark.sql.functions.*;

import java.util.List;
import java.util.Objects;

import edu.campus02.iwi.demo.wc.WordCountJava8RDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import spark.exercise.env.WinConfig;

public class LogAnalyzerDSL {

	public static void main(String[] args) {

		WinConfig.setupEnv();
		
		if (args.length != 1) {
			System.err.println("usage: program <path_to_apache_log_file>");
			System.exit(-1);
		}

		//TODO 1: create spark config & session instead of context
		SparkConf cnf = new SparkConf().setMaster("local[1]")
				.setAppName(LogAnalyzerDSL.class.getName());
		SparkSession spark = SparkSession.builder()
				.config(cnf)
				.getOrCreate();
		
		//TODO 2: read in the access log file with string encoder
		//and map the elements with the bean encoder to ApacheAccessLog objects
		//we also filter them in order to keep only non-null entries
		Dataset<ApacheAccessLog> accessLogs = spark.read().text(args[0]).as(Encoders.STRING()).
				map(ApacheAccessLog::parseFromLogLine, Encoders.bean(ApacheAccessLog.class))
				.filter(Objects::nonNull).cache();
		
		//TODO 3: inspect the schema and a few sample records
		accessLogs.printSchema();
		accessLogs.show(10);
		System.out.println(accessLogs.count());

		//TODO 4: simple grouping by HTTP method
		RelationalGroupedDataset groupedMethods = accessLogs.groupBy("method");
		Dataset<Row> countedGroups = groupedMethods.count();
		countedGroups.show();


		//TODO 5: calculate and show the top 25 requested resources (=endpoints)
		RelationalGroupedDataset groupedEndpoints = accessLogs.groupBy("endpoint");
		Dataset<Row> countedEndpoints = groupedEndpoints.count();
		Dataset<Row> orderedEndpoints = countedEndpoints.orderBy(col("count").desc());
		orderedEndpoints.show(25);
		
		//TODO 6: collect all hosts (=ipAddress) that have accessed
		//the server more than 250 times into  list and it to the console
		RelationalGroupedDataset groupedHosts = accessLogs.groupBy("ipAddress");

		Dataset<Row> countedHosts = groupedHosts.count();
		Dataset<Row> filteredHosts = countedEndpoints.filter("count > 250");
		List<Row> hostList = filteredHosts.collectAsList();
		System.out.println(hostList);

		//TODO 7: apply the aggregate functions avg, min, max, sum
		//based on contentSize column of requested resources
		//and show the results
		accessLogs.agg(avg("contentSize"), max("contentSize"), min("contentSize"), sum("contentSize")).show();

	}
}