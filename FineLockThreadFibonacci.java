package edu.neu.mapreduce.assignments.assignment1; 

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/*
 * FineLockThreadFibonacci extends Thread class and computes Average TMAX temperatures for each stationID 
 * of the parsed data and accumulates the results in the shared data structure stationAvgTemp
 * with the finse lock feature being implemented
 * It adds a delay of fibonacci(17) while updating the data structure
 */
public class FineLockThreadFibonacci extends Thread {
	public final static Logger logger = Logger.getLogger(FineLockThreadFibonacci.class.getName());
	// A special data structure that avoids data races is being used
	ConcurrentHashMap<String, StationAvgTempEntry> stationAvgTemp;
	public List<String> tmax_records = new ArrayList<String>();//list data structure to store the TMAX station records
	
	String name; 
	List<String> records = new ArrayList<String>();
	int low;
	int high;

	/*
	 * Constructor:
	 * @arg1: records - Complete list of records obtained from parsing 1912.csv file
	 * @arg2: low - The start index of the chunk of entire data to be processed by the thread
	 * @arg3: high - The end index of the chunk of entire data to be processed by this instance of thread
	 * @arg4: stationAvgTemp - The accumulation data structure declared and defined in the main thread
	 * 	      and shared by all child threads. 
	 */
	FineLockThreadFibonacci(String threadname, List<String> records, int low, int high, ConcurrentHashMap<String, StationAvgTempEntry> stationAvgTemp) {
		name = threadname;
		for(int i=low;i<=high;i++){
			this.records.add(records.get(i));
		}
		
		this.low=low;
		this.stationAvgTemp = stationAvgTemp;
		this.high=high;
	}
	
	/*
	 * getStationAllTemperatures: Method to parse all the records whose Entry type is "TMAX"
	 * It splits each record by a delimiter(,) and checks of the stationID being processed already
	 * exists in the stationAvgTemp. 
	 * If it exists, add the non null temperature value of the current record to the already existing
	 * sum, and thereby modify the average and count of the list of temperatures for the given stationID
	 * If it does not exists, add a new entry to the stationAvgTemp data structure with the current 
	 * StationID as the key and a new StationAvgTempEntry object as the value
	 * 
	 * Also adds a delay of fibonacci(17) when the shared data structure is updated
	 * 
	 * FINELOCK: Lock is applied on only the segment of the data structure "stationAvgTemp" when it is being
	 * updated with the new avg and count value. 
	 * Lock is applied on both conditions - stationID exists or do not exists 
	 * 
	 */
	public void getStationAllTemperatures(){
		for(String s : this.tmax_records){
			String[] values = s.split(",");
				if(stationAvgTemp.containsKey(values[0])){
					if(values[3] != null || values[3]!= ""){
						/* lock being added only to the value of the entry in the stationAvgTemp data
						* structure with values[0] as its key
						* along with the delay being added
						*/
						synchronized(stationAvgTemp.get(values[0])){
							stationAvgTemp.get(values[0]).computeAvgTemp(Integer.parseInt(values[3]));
			    			fibonacci(17);
			       		 }
			    	 }

			     } else {
			    	 stationAvgTemp.put(values[0], new StationAvgTempEntry());
			    	 /* lock being added only to the value of the entry in the stationAvgTemp data
					  * structure with values[0] as its key
					  * along with the delay being added
					 */
			    	 synchronized(stationAvgTemp.get(values[0])){
			    		 stationAvgTemp.get(values[0]).computeAvgTemp(Integer.parseInt(values[3]));
			    		 fibonacci(17);
			       	 }
			    } 
		 }
	}
	
	/*
	 * fibonacci: method to compute the fibonacci of a given number
	 * @arg1: n - the number n whose fibonacci has to be calculated
	 */
	public long fibonacci(int n) {
		if (n <= 1) return n;
	    	else return fibonacci(n-1) + fibonacci(n-2);
	}
	 
	/*
	 * @Overriding run() method of Thread class 
	 * Creates an object of FindMAXRecords class
	 * Invokes findTMAXrecords method and accumulates only the records with TMAX as the entry type
	 * which will be further processed by getStationAllTemperatures() method
	 */
	public void run() {
		FindMAXRecords find = new FindMAXRecords();
		//logger.info(this.name + " begin computation of avg temperature for each station ID.");
		this.tmax_records = find.findTMAXrecords(this.records, this.tmax_records);
		getStationAllTemperatures();
	}
}
