 package edu.campus02.iwi.demo.gfgx;

import org.apache.spark.SparkConf;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import scala.Tuple2;
import spark.exercise.env.WinConfig;

import static org.apache.spark.sql.functions.*;

public class OpenFlightsDataGX {

	public static void main(String[] args) {
		
		WinConfig.setupEnv();
		
		SparkConf cnf = new SparkConf().setMaster("local")
				.setAppName("UE Graphs FH C02");
		
		SparkSession spark = SparkSession.builder()
							     .config(cnf)
							     .getOrCreate();
	
		//make from GraphFrame API...
		Dataset<Row> airports = spark.read()
				.option("header",true)
				.option("delimiter",",")
				.option("nullValue", "\\N")
				.option("inferSchema",true)
				.csv("data/input/lv/openflights/airports.dat");

		airports.show(5);
		
		Dataset<Row> routes = spark.read()
				.option("header",true)
				.option("delimiter",",")
				.option("nullValue", "\\N")
				.option("inferSchema",true)
				.csv("data/input/lv/openflights/routes.dat");

		routes.show(5);
		
		//ID as vertex id and edge connection
		GraphFrame gf = new GraphFrame(airports.selectExpr("AirportID as id","City","IATA"),
							routes.selectExpr("SourceAirportID as src","DestinationAirportID as dst","AirlineID").na().drop()
								);
		
		Graph<Row,Row> gx = gf.toGraphX().cache();
		
		System.out.println("vertices: "+gx.vertices().count());
		System.out.println("edges: "+gx.edges().count());

		//access some edge triplets
		gx.triplets().toJavaRDD()
			.map(et -> "from: "+ et.srcAttr() 
					+ " -> with: "+et.attr().getInt(2)
					+ " -> to: "+et.dstAttr())
		 	.take(25).forEach(System.out::println);
		
		//simple graph operators
		
		//find isolated airports (degree 0, no in/out flights)
		
		//airports with no outflights
		gx.ops().outDegrees().toJavaRDD()
				.filter(t -> ((Integer)t._2) == 0).collect()
				.forEach(System.out::println);
		
		//airports with no inflights
		gx.ops().inDegrees().toJavaRDD().filter(t -> ((Integer)t._2) == 0).collect()
				.forEach(System.out::println);
			
		//find top 25 airports with most outgoing flights
		
		gx.ops()
			//Tuple (vertexId,outDegree)
			.outDegrees().toJavaRDD().mapToPair(t -> t)
			//Tuple (vertextId, Tuple(outDegree,Row[vertexId, City, IATA])
			.join(gx.vertices().toJavaRDD().mapToPair(t -> t))
			//Tuple (outDegree, Row[vertexId, City, IATA]
			.mapToPair(t -> new Tuple2<>(t._2._1, t._2._2))
			.sortByKey(false)
			.take(25)
			.forEach(System.out::println);
			
		spark.close();

	}

}
