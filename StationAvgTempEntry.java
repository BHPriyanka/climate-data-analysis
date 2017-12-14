package edu.neu.mapreduce.assignments.assignment1;

/*
 * StationAvgTempEntry class which represents an entry corresponding to each StationID
 * @field1: avg - Variable holding the average value of all the TMAX temperatures for the specific Station
 * @field2: count - Variable holding the number of TMAX temperatures for the specified station
 */
public class StationAvgTempEntry {
	float avg;
	int count;
	
	StationAvgTempEntry(){
		avg=0;
		count=0;
	}
	
	/*
	 * computeAvgTemp: Method to compute the new average and count values by conidering the new temperature
	 * @arg1: maxValue - Temperature of the TMAX station record being processed
	 */
	public void computeAvgTemp(int maxValue){
		float sum = avg * count + maxValue;
		count++;
		avg =  sum/count;
	}
	
	/*
	 * remove: Method to clear the avg and count of each station entry
	 */
	public void remove(){
		count=0;
		avg=0;
	}
}
