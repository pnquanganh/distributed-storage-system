package ce7490_master;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;

import client_master_interface.*;


public class Master implements client_master_interface{
	public static HashMap<String, Reading_request_result> files;
	public static HashMap<Info, HashSet<String>> slaves;
	public static HashSet<Info> deadSlaves;
	
	public static Info getNextSlave() {		
		return null;
	}
	
	public static void main(String[] args)
	{
		files = new HashMap<String, Reading_request_result>();
		slaves = new HashMap<Info, HashSet<String>>();
		deadSlaves = new HashSet<Info>();
	}
	
	public static void recovery()
	{
		HashSet<String> damagedFiles = new HashSet<String>();
		
		for (Info dSlave : deadSlaves){
			damagedFiles.addAll(slaves.get(dSlave));
			slaves.remove(dSlave);
		}
		
		for (String dFile : damagedFiles){
			Reading_request_result fileInfo =  files.get(dFile);
			HashMap<Hierachical_codes, Info> relatedSlaves = fileInfo.slaves;
			
			
			
		}
		
		deadSlaves.clear();
	}
	
	

	@Override
	public Writing_request_result get_writing_slaves(String filename,
			int filesize) throws RemoteException {
		// TODO Auto-generated method stub
		HashMap<Hierachical_codes, Info> fileslaves = new HashMap<Hierachical_codes, Info>();
		for (Hierachical_codes i: Hierachical_codes.values()){
			Info newSlave = getNextSlave();
			fileslaves.put(i, newSlave);		 
			HashSet<String> fileinSlave = slaves.get(newSlave);
			fileinSlave.add(filename);
		}
		
		Reading_request_result newFile = new Reading_request_result();
		newFile.file_size = filesize;
		newFile.slaves = fileslaves;
		files.put(filename, newFile);
		
		Writing_request_result writeResponse = new Writing_request_result();
		writeResponse.slaves = fileslaves;
		return writeResponse;
	}

	@Override
	public Reading_request_result get_reading_slaves(String filename)
			throws RemoteException {
		// TODO Auto-generated method stub
		return files.get(filename);
	}
	
	
	
}
