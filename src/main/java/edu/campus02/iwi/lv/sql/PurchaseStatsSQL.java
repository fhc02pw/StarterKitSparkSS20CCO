package edu.campus02.iwi.lv.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import spark.exercise.env.WinConfig;

public class PurchaseStatsSQL {

	public static void main(String[] args) {

		WinConfig.setupEnv();
		
		if (args.length != 1) {
			System.err.println("usage: program <path_to_purchase_json>");
			System.exit(-1);
		}

		//TODO 1: create spark config & session instead of context
		
		//TODO 2: read in json data file as a dataframe (=Dataset<Row>)
		//and directly cache it...
		
		//TODO 3: register a temporary view named "buyings"
		//which can later be used in our SQL statements...

		//TODO 4: inspect the schema of the dataframe and show some records

		//NOW LETS WRITE SOME good old plain SQL :)
		
		//TODO 5: write SQL to get the number of records
		//grouped for each payment type

		
		//TODO 6: write SQL to find the categories AVG orderTotal for any purchases
		//within the productCategory "Music" or "Books" which were paid
		//by "Visa"
		
		//TODO 7: write SQL to find highest and lowest orderTotal for any purchases
		//done in "San Diego" which were paid by "Cash"

	}

}
