package edu.neu.mapreduce.assignments.assignment1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

/*
 * NoShareThread extends Thread class and computes Average TMAX temperatures for each stationID 
 * of the parsed data and accumulates the results in its own data structure stationAvgTemp
 */
public class NoShareThread extends Thread {
	public final static Logger logger = Logger.getLogger(NoShareThread.class.getName());
	List<String> tmax_records = new ArrayList<String>();//list data structure to store the TMAX station records
	
	// Data structure to store the average temperature and count for each station ID
	HashMap<String, StationAvgTempEntry> stationAvgTemp = new HashMap<String, StationAvgTempEntry>(); 
	
	String name; 
	List<String> records = new ArrayList<String>();
	int low;
	int high;

	/*
	 * Constructor:
	 * @arg1: records - Complete list of records obtained from parsing 1912.csv file
	 * @arg2: low - The start index of the chunk of entire data to be processed by the thread
	 * @arg3: high - The end index of the chunk of entire data to be processed by this instance of thread
	 */
	NoShareThread(String threadname, List<String> records, int low, int high) {
		this.name = threadname;
		for(int i = low; i <= high; i++){
			this.records.add(records.get(i));
		}
		this.low=low;
		this.high=high;
	}

	/*
	 * getStationAllTemperatures: Method to parse all the records whose Entry type is "TMAX"
	 * It splits each record by a delimiter(,) and checks of the stationID being processed already
	 * exists in the local stationAvgTemp. 
	 * If it exists, add the non null temperature value of the current record to the already existing
	 * sum, and thereby modify the average and count of the list of temperatures for the given stationID
	 * If it does not exists, add a new entry to the stationAvgTemp data structure with the current 
	 * StationID as the key and a new StationAvgTempEntry object as the value
	 */
	public void getStationAllTemperatures(){
		for(String s : this.tmax_records){
			String[] values = s.split(",");
				if(stationAvgTemp.containsKey(values[0])){
					if(values[3] != null || values[3]!= ""){
						stationAvgTemp.get(values[0]).computeAvgTemp(Integer.parseInt(values[3]));
			         }
			    } else {
			    	stationAvgTemp.put(values[0], new StationAvgTempEntry());
			        stationAvgTemp.get(values[0]).computeAvgTemp(Integer.parseInt(values[3]));
			    } 
		}
	}
	
	/*
	 * getResults: Method to return the thread instance's stationAvgTemp
	 */
	public HashMap<String, StationAvgTempEntry> getResults() {
       return this.stationAvgTemp;
    }
	 
	/*
	 * @Overriding run() method of Thread class 
	 * Creates an object of FindMAXRecords class
	 * Invokes findTMAXrecords method and accumulates only the records with TMAX as the entry type
	 * which will be further processed by getStationAllTemperatures() method
	 */
	public void run() {
		FindMAXRecords find = new FindMAXRecords();
		//logger.info(this.name + " begin computation of max temperature for each station ID.");
		this.tmax_records = find.findTMAXrecords(this.records, this.tmax_records);
		getStationAllTemperatures();
	}
}

