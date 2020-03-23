package edu.campus02.iwi.lv.graph;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import spark.exercise.env.WinConfig;

public class OpenFlightsAnalyzer {

	public static void main(String[] args) {
		
		WinConfig.setupEnv();
		
		//TODO 1: spark config and session

		//TODO 2:
		//a) airports DF
		
		//b) airlines DF
		
		//c) routes DF
				
		//TODO 3: create GF based on DFs
		
		//TODO 4:
		//TOP 10 US airports having highest number of incoming flights
		
		//TODO 5: all flights within Austria only
			//V1:
			//a) filter for airports in Austria
			
			//b) JOIN to the routes DF twice the airport DF (1x for src, 1x for dst)
			//then the airline DF 
	
			//V2:
			//motif-finding and 1 join enrichment
		
		//TODO 6 BONUSTASK
		//all flights from VIE -> SFO but with either Lufthansa/Austrian/Swiss and 2 hops only

	}

}
