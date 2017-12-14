package edu.neu.mapreduce.assignments.assignment1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/*
 * NoSharingVersionFibonacci: 
 *  	a. loads input file into ArrayList named as "records"
 *  	b. computes the number of processors supported by the system using 
 *  	   Runtime.getRuntime().availableProcessors
 * 		c. spawns 4 threads
 * 		d. divides input into 4 equal size partitions
 * 		e. assign equal work to each thread
 * 		f. refer NoShareThreadThreadFibonacci.java for thread details 
 *  	g. logs the execution time for ten executions
 *  	h. adds a delay of fib(17) when the data structure is being updated
 */
public class NoSharingVersionFibonacci {
	public final static Logger logger = Logger.getLogger(NoSharingVersionFibonacci.class.getName());
	// Accumulation data structure being considered for the final merging of results of  
	// individual threads
	HashMap<String, StationAvgTempEntry> stationAvgTempMap = new HashMap<String, StationAvgTempEntry>();
	
	
	/*
	 * divideRecordsEqually: Divides the load equally for 4 threads
	 * Each new thread being spawned is added to the ArrayList fourThreads
	 * @arg1: records - list of entire set of records obtained from 1912.csv
	 */
	public void divideRecordsEqually(List<String> records){
		ArrayList<HashMap<String, StationAvgTempEntry>> finalResults = new ArrayList<HashMap<String, StationAvgTempEntry>>();
		int cores = Runtime.getRuntime().availableProcessors();
		int size = records.size();
		int chunk_size = size / cores;    
		
		// assign data starting from 0-chunk_size-1, chunk_size-2*chunk_size-1, and so on for 4 cores of processor
		//create 4 Thread using for loop
		List<NoShareThreadFibonacci> fourThreads = new ArrayList<NoShareThreadFibonacci>();
		int low;
		int high;
        for (int x = 0; x < cores; x++){
        	low = x*chunk_size;
        	if(x == cores-1){
        		high = size-1;
        	} else {
        		high= (x+1)*chunk_size -1;
        	}
            	
        	// Creating NoShareThreadFibonacci thread
            fourThreads.add(new NoShareThreadFibonacci("Thread " + x, records, low, high));
        }
        
        // Starts each of the threads created
        for(int i = 0; i < fourThreads.size(); i++){
        	fourThreads.get(i).start();
        }
        
        // Waits for each of threads for completion using join()
        try {
        	for(int i = 0;i < cores;i++){
        		fourThreads.get(i).join();
        	}
        }catch(InterruptedException ie ){
        	System.out.println(ie);
        }
        
        for(int i=0; i< cores;i++){
        	finalResults.add(fourThreads.get(i).getResults());
        }
    	
        mergeFinalResultsOfThreads(finalResults);
     }

	
	/*
	 * mergeFinalResultsOfThreads: Method as the name suggests merges all the individual 
	 * thread results into a single data structure of this class
	 * @arg: finalResults - an arraylist of hashmaps with stationID as the key and StationAvgTempEntry as value
	 * 
	 * a. Iterates over each thread result
	 * b. fetches each Hashmap entry
	 * c. obtains the count and average of current entry being processed
	 * d. checks if the common data structure "stationAvgTempMap" has the stationID being checked
	 * e. If it exists, fetches the stored count and average values
	 * f. updates the hashmap entry with new average and count values
	 * g. If id does not exist, add a new entry 
	 */
	public void mergeFinalResultsOfThreads(ArrayList<HashMap<String, StationAvgTempEntry>> finalResults){
		for(int i=0;i<finalResults.size();i++){
			for(Map.Entry<String,StationAvgTempEntry> entry: finalResults.get(i).entrySet()){
				int EntryCount = entry.getValue().count;
			    float EntryAvg = entry.getValue().avg;
				if(stationAvgTempMap.containsKey(entry.getKey())){
					StationAvgTempEntry storedEntry = stationAvgTempMap.get(entry.getKey());
					int count = storedEntry.count;
					float avg = storedEntry.avg;
					int totalCount = EntryCount + count;
					float totalAvg = ((EntryCount * EntryAvg) + (count *avg))/totalCount;
					storedEntry.count = totalCount;
					storedEntry.avg = totalAvg;
					stationAvgTempMap.put(entry.getKey(), storedEntry);
				} else {
					stationAvgTempMap.put(entry.getKey(), entry.getValue());
				}
			}
		}
	
	}

	/*
	 * NO_SHARING_FIB_version: Method to execute the basic functionality divideRecordsEqually of NoSharingVersion class
	 * 10 times, print the results, and log in the minimum, average and maximum execution
	 * times for these 10 executions
	 * @arg1: records - Complete list of records as obtained from parsing 1912.csv input file
	 */
	public void NO_SHARING_FIB_version(List<String> records){
		ArrayList<Long> executionTimes = new ArrayList<Long>();
		for(int i=0;i<10;i++){
			stationAvgTempMap.clear();
			long startTime = System.currentTimeMillis();

			divideRecordsEqually(records);

			long endTime = System.currentTimeMillis();
		    executionTimes.add(endTime-startTime);
			if(i == 0){
				OutputWriter oW = new OutputWriter(stationAvgTempMap, "NoSharingVersionFibonacciOutput");
			   	oW.writeResults();
			}
            
		    for(Map.Entry<String, StationAvgTempEntry> entry: stationAvgTempMap.entrySet()){
		    	entry.getValue().remove();
		    }
		    stationAvgTempMap.clear();
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
	
		NoSharingVersionFibonacci noSharingFib = new NoSharingVersionFibonacci();
		noSharingFib.NO_SHARING_FIB_version(load.records);
	}
}

