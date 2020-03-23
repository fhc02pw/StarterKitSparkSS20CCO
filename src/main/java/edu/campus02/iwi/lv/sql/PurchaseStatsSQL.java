package edu.campus02.iwi.lv.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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
		SparkConf conf = new SparkConf().setMaster("local").setAppName(PurchaseStatsSQL.class.getName());
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		//TODO 2: read in json data file as a dataframe (=Dataset<Row>)
		//and directly cache it...
		Dataset<Row> buyingsJson = spark.read().json(args[0]).cache();

		//TODO 3: register a temporary view named "buyings"
		//which can later be used in our SQL statements...
		buyingsJson.createOrReplaceTempView("buyings");


		//TODO 4: inspect the schema of the dataframe and show some records
		buyingsJson.printSchema();
		buyingsJson.show(10);

		//NOW LETS WRITE SOME good old plain SQL :)
		//TODO 5: write SQL to get the number of records
		//grouped for each payment type
		Dataset<Row> groupedPaymentType = spark.sql("SELECT paymentType, count(*) FROM buyings GROUP BY paymentType");
		groupedPaymentType.show();
		
		//TODO 6: write SQL to find the categories AVG orderTotal for any purchases
		//within the productCategory "Music" or "Books" which were paid
		//by "Visa"
		spark.sql("SELECT productCategory, AVG(orderTotal) FROM buyings WHERE paymentType = 'Visa' AND productCategory IN ('Music', 'Books')" +
				"GROUP BY productCategory")
				.show();
		
		//TODO 7: write SQL to find highest and lowest orderTotal for any purchases
		//done in "San Diego" which were paid by "Cash"
		spark.sql("SELECT MIN(orderTotal), MAX(orderTotal) FROM buyings WHERE paymentType = 'Cash' AND buyingLocation = 'San Diego'")
				.show(); 
	}

}
