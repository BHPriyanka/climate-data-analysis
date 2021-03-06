package edu.neu.mapreduce.assignments.assignment1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/*
 * CoarseLockVersionFibonacci: 
 *  	a. loads input file into ArrayList named as "records"
 *  	b. computes the number of processors supported by the system using 
 *  	   Runtime.getRuntime().availableProcessors
 * 		c. spawns 4 threads
 * 		d. divides input into 4 equal size partitions
 * 		e. assign equal work to each thread
 * 		f. threads are synchronized by applying a lock on the entire accumulation data structure
 * 		g. refer CoarseLockThreadFibonacci.java for thread details 
 *  	h. logs the execution time for ten executions
 *  	i. adds a delay of fib(17) when the data structure is being updated
 */
public class CoarseLockVersionFibonacci {
	public final static Logger logger = Logger.getLogger(CoarseLockVersionFibonacci.class.getName());
	
	// Accumulation data structure being considered for the parellel updates of 
	// average and count of tmax temperatures
	public HashMap<String, StationAvgTempEntry> stationAvgTemp = new HashMap<String, StationAvgTempEntry>();
	
	/*
	 * divideRecordsEqually: Divides the load equally for 4 threads
	 * Each new thread being spawned is added to the ArrayList fourThreads
	 * @arg1: records - list of entire set of records obtained from 1912.csv
	 */
	public void divideRecordsEqually(List<String> records){
		int cores = Runtime.getRuntime().availableProcessors();
		int size = records.size();
		int chunk_size = size / cores;    
		
		// assign data starting from 0-chunk_size-1, chunk_size-2*chunk_size-1, and so on for  cores of processor
		// create 4 threads
		List<CoarseLockThreadFibonacci> fourThreads = new ArrayList<CoarseLockThreadFibonacci>();
		int low;
		int high;
        for (int x = 0; x < cores; x++) {
        	low = x*chunk_size;
        	if(x == cores-1){
        		high = size-1;
        	} else {
        		high= (x+1)*chunk_size -1;
        	}
            	
        	// Creating CoarseLockThreadFibonacci thread
            fourThreads.add(new CoarseLockThreadFibonacci("Thread " + x, records, low, high, stationAvgTemp));
        }      
        
        // Starts each of the threads created
        for(int i = 0;i < fourThreads.size();i++){
        	fourThreads.get(i).start();
        }
        
        // Waits for each of threads for completion using join()
        try {
        	for(int i = 0;i < cores; i++){
        		fourThreads.get(i).join();
        	}
        }catch(InterruptedException ie ){
        	System.out.println(ie);
        }
	}

	/*
	 * COARSE_LOCK_FIB_version: Method to execute the basic functionality divideRecordsEqually of CoarseLockVersionFibonacci class
	 * 10 times, print the results, and log in the minimum, average and maximum execution
	 * times for these 10 executions
	 * @arg1: records - Complete list of records as obtained from parsing 1912.csv input file
	 */
	public void COARSE_LOCK_FIB_version(List<String> records){
		ArrayList<Long> executionTimes = new ArrayList<Long>();
		for(int i=0;i<10;i++){
			stationAvgTemp.clear();
			long startTime = System.currentTimeMillis();

			divideRecordsEqually(records);

			long endTime = System.currentTimeMillis();
			executionTimes.add(endTime-startTime);
		     
			if(i == 0){
				OutputWriter oW = new OutputWriter(stationAvgTemp, "CoarseLockVersionFibonacciOutput");
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
	
	public static void main(String[] args){
		LoadCSVData load = new LoadCSVData();
		load.loader();
		
		CoarseLockVersionFibonacci coarseLockFib = new CoarseLockVersionFibonacci();
		coarseLockFib.COARSE_LOCK_FIB_version(load.records);	
	}
}


