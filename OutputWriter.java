package edu.neu.mapreduce.assignments.assignment1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * OutputWriter
 * 			a. writes the output of the specified version to a given file
 */
public class OutputWriter {
	
	HashMap<String, StationAvgTempEntry> stationAvgTemp = new HashMap<String, StationAvgTempEntry>();
	ConcurrentHashMap<String, StationAvgTempEntry> conStationAvgTemp = new ConcurrentHashMap<String, StationAvgTempEntry>();
	String filename = new String();
	
	
	/*
	 * Constructor to initialize the input from where the data needs to be read and the 
	 * destination filename
	 */
	OutputWriter(HashMap<String, StationAvgTempEntry> stationAvgTemp, String filename){
		this.stationAvgTemp = stationAvgTemp;
		this.filename = filename;
	}
	
	
	/*
	 * Constructor for FineLock versions to initialize the input from where the data needs to be read and the 
	 * destination filename
	 */
	OutputWriter(ConcurrentHashMap<String, StationAvgTempEntry> stationAvgTemp, String filename){
		this.conStationAvgTemp = stationAvgTemp;
		this.filename = filename;
	}
	
	
	/*
	 * writeOutputToFile: Method to write the output of the program to a file 
	 * inside output directory
	 * @args1: the content to be written to a file
	 */
	public void writeOutputToFile(String content){
		BufferedWriter bw = null;
		FileWriter fw = null;

		try {
			fw = new FileWriter("./output/"+filename, true);
			bw = new BufferedWriter(fw);
			bw.write(content);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();

			} catch (IOException ex) {

				ex.printStackTrace();
			}
		}
	}
	
	
	
	/*
	 * writeResults: Method which parses the given input data structure and invokes writeOutputFile
	 * for every single entry
	 * It also writes the total number of resultant records
	 */
	public void writeResults(){
		File dir = new File("./output");
		
		if(!dir.exists()){
			dir.mkdir();
		}
		
		File file = new File("./output/" +filename);
		
		if(file.exists()){
			file.delete();
		}
		
		if(filename.contains("FineLock")){
			for(Map.Entry<String, StationAvgTempEntry> entry: conStationAvgTemp.entrySet()){
				writeOutputToFile(entry.getKey() +" "+entry.getValue().avg +"\n");
			}
			writeOutputToFile("Number of records: " + conStationAvgTemp.size());
		}else {
			for(Map.Entry<String, StationAvgTempEntry> entry: stationAvgTemp.entrySet()){
				writeOutputToFile(entry.getKey() +" "+entry.getValue().avg +"\n");
			}
			writeOutputToFile("Number of records: " + stationAvgTemp.size());
		}
	}
}
