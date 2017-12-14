package edu.neu.mapreduce.assignments.assignment1;

import java.util.List;

/*
 * FindMAXRecords class parses the entire set of records and accumulates only the records with TMAX entry type
 * and ignores all the other entry types(PRCP,TMIN,etc)
 */
public class FindMAXRecords {

	//method to parse through each string returned by loader module and check whether the third entry is TMAX
	public List<String> findTMAXrecords(List<String> records, List<String> dest_list){
	
	   for(String s : records){
	     //split the string based on ,
	     String[] values = s.split(",");
	     	     
	     if(values[2].equals("TMAX"))
	     {	 
	         dest_list.add(s);
	     }
	   }
	   return dest_list;
	}
}
