package edu.neu.mapreduce.assignments.assignment1;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class MainProgram {
	public final static Logger logger = Logger.getLogger(MainProgram.class.getName());
	public static List<String> records = new ArrayList<String>();
	
	public static void main(String[] args){
		LoadCSVData load = new LoadCSVData();
		logger.info("Parsing data...");
		load.loader();
		records = load.records;
		
		logger.info("Instantiating Sequential version");
		SequentialVersion sqVer = new SequentialVersion();
		sqVer.SEQ_version(records);
		
		logger.info("Instantiating NoLock version");
		NoLockVersion noLock = new NoLockVersion();
		noLock.NO_LOCK_version(records);	
		
		logger.info("Instantiating CoarseLock version");
		CoarseLockVersion coarseLock = new CoarseLockVersion();
		coarseLock.COARSE_LOCK_version(records);
		
		logger.info("Instantiating FineLock version");
		FineLockVersion fineLock = new FineLockVersion();
		fineLock.FINE_LOCK_version(records);
		
		logger.info("Instantiating NoSharing version");
		NoSharingVersion noSharing = new NoSharingVersion();
		noSharing.NO_SHARING_version(records);
		
		logger.info("Instantiating Sequential version with a delay of fibonacci(17)");
		SequentialVersionFibonacci sqVerFib = new SequentialVersionFibonacci();
		sqVerFib.SEQ_FIB_version(records);
		
		logger.info("Instantiating NoLock version with a delay of fibonacci(17)");
		NoLockVersionFibonacci noLockFib = new NoLockVersionFibonacci();
		noLockFib.NO_LOCK_FIB_version(records);	
		
		logger.info("Instantiating CoarseLock version fibonacci(17)");
		CoarseLockVersionFibonacci coarseLockFib = new CoarseLockVersionFibonacci();
		coarseLockFib.COARSE_LOCK_FIB_version(records);
		
		logger.info("Instantiating FineLock version with a delay of fibonacci(17)");
		FineLockVersionFibonacci fineLockFib = new FineLockVersionFibonacci();
		fineLockFib.FINE_LOCK_FIB_version(records);	
		
		logger.info("Instantiating NoSharing version with a delay of fibonacci(17)");
		NoSharingVersionFibonacci noSharingFib = new NoSharingVersionFibonacci();
		noSharingFib.NO_SHARING_FIB_version(records);
		
	}
	
}
