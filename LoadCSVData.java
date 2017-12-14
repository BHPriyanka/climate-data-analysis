package edu.neu.mapreduce.assignments.assignment1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.logging.Level;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/*
 * LoadCSVData class to parse entire the input file 1912.csv and store each line
 * as a string  into a Arraylist
 */
public class LoadCSVData {
	private final static Logger logger = Logger.getLogger(LoadCSVData.class.getName());
	public List<String> records = new ArrayList<String>();
	
	public void loader() {
		String filename = "input/1912.csv";
			
		//reads the file and returns a String[] or List<String> containing lines of file
		BufferedReader br = null;
		
		try{
			br = new BufferedReader(new FileReader(filename));
			String line;
			while((line = br.readLine()) != null) {
				records.add(line);       
			}
		} 
		catch(FileNotFoundException fe){
			logger.log(Level.SEVERE, fe.toString());
		}
		catch(IOException ie){
			logger.log(Level.SEVERE, ie.toString());
		} 
		finally{
			try{
				if(br !=null){
					br.close();
					logger.info("1912.csv file processed and stored as List<String>");
				}
			} catch(IOException ie){
				System.out.println(ie);
			}
		}
	}
}
