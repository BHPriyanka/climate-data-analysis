package edu.neu.mapreduce.assignments.assignment1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/*
 * SequentialVersionFibonacci: 
 *  	a. loads input file into ArrayList named as "records"
 * 		b. refer SequentialThreadFibonacci.java for thread details 
 *  	c. logs the execution time for ten executions
 *  	d. Sequential executes the task of calculating the average of all tmax temperatures for each station
 *  	e. adds a delay of fib(17) while updating the data structure
 */
public class SequentialVersionFibonacci {
	private final static Logger logger = Logger.getLogger(SequentialVersionFibonacci.class.getName());
	public List<String> tmax_records = new ArrayList<String>();
	
	// Accumulation data structure being considered for the parellel updates of 
	// average and count of tmax temperatures. It avoids data races
	public HashMap<String, StationAvgTempEntry> stationAvgTemp = new HashMap<String, StationAvgTempEntry>();
	
	/*
	 * getStationAllTemperatures: Method to parse all the records whose Entry type is "TMAX"
	 * It splits each record by a delimiter(,) and checks of the stationID being processed already
	 * exists in the stationAvgTemp. 
	 * If it exists, add the non null temperature value of the current record to the already existing
	 * sum, and thereby modify the average and count of the list of temperatures for the given stationID
	 * If it does not exists, add a new entry to the stationAvgTemp data structure with the current 
	 * StationID as the key and a new StationAvgTempEntry object as the value 
	 */
	public void getStationAllTemepratures(){
		for(String s : tmax_records){
			String[] values = s.split(",");
				if(stationAvgTemp.containsKey(values[0])){
					if(values[3] != null || values[3]!= ""){
						stationAvgTemp.get(values[0]).computeAvgTemp(Integer.parseInt(values[3]));
						//adds a delay of fibonacci(17)
			           	fibonacci(17);
			         }
				} else {
					stationAvgTemp.put(values[0], new StationAvgTempEntry());
			        stationAvgTemp.get(values[0]).computeAvgTemp(Integer.parseInt(values[3]));
			        //adds a delay of fibonacci(17)
			        fibonacci(17);
			     } 
			  }
	}


	/*
	 * SEQ_FIB_version: Method to execute the basic functionality of
	 * a. fetching tmax records by invoking method of FindMAXRecords class object
	 * b. iterating over each tmax record to compute the average temperature for each station along 
	 *    with its count
	 *    
	 * Executes the task 10 times, print the results, and log in the minimum, average and maximum execution
	 * times for these 10 executions
	 * @arg1: records - Complete list of records as obtained from parsing 1912.csv input file
	 */
	public void SEQ_FIB_version(List<String> records){
		FindMAXRecords find = new FindMAXRecords();
		ArrayList<Long> executionTimes = new ArrayList<Long>();
		for(int i=0;i<10;i++){
			stationAvgTemp.clear();	
			long startTime = System.currentTimeMillis();
			
			tmax_records = find.findTMAXrecords(records, tmax_records);
			getStationAllTemepratures();
	
			long endTime = System.currentTimeMillis();
			executionTimes.add(endTime-startTime);
			if(i == 0){
				OutputWriter oW = new OutputWriter(stationAvgTemp, "SequentialVersionFibonacciOutput");
			   	oW.writeResults();
			}
			   
			for(Map.Entry<String, StationAvgTempEntry> entry: stationAvgTemp.entrySet()){
		      	entry.getValue().remove();
		    }
		 	stationAvgTemp.clear();	
		}
		
		long sumTimes = 0;
		for(long f: executionTimes){
			sumTimes = sumTimes + f;
		}
		
		System.out.println("Average execution time: " + sumTimes/executionTimes.size());
		System.out.println("Minimum execution time: "+ Collections.min(executionTimes));
		System.out.println("Maximum execution time: "+ Collections.max(executionTimes));
	}
	
	public long fibonacci(int n) {
		if (n <= 1) return n;
	    	else return fibonacci(n-1) + fibonacci(n-2);
	}
	  
	public static void main(String[] args){
		LoadCSVData load = new LoadCSVData();
		SequentialVersionFibonacci sqVerFib = new SequentialVersionFibonacci();
		load.loader();
		
		sqVerFib.SEQ_FIB_version(load.records);
	}
}


