package ce7490_master;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import hierachical_code.*;
import client_master_interface.*;

public class testclass implements client_master_interface{
	public static HashMap<String, Reading_request_result> files;
	public static HashMap<Integer, HashSet<String>> slaves;
	
	public static slave_info getNextSlave() {		
		return null;
	}

	@Override
	public Writing_request_result get_writing_slaves(String filename,
			int filesize) throws RemoteException {
		// TODO Auto-generated method stub
		for (int i = 0; i<7; i++){
			slave_info slave1 = getNextSlave();
			
		}
		return null;
	}

	@Override
	public Reading_request_result get_reading_slaves(String filename)
			throws RemoteException {
		// TODO Auto-generated method stub
		return files.get(filename);
	}
	
	public static void main(String[] args){
		slaves = new HashMap<Integer, HashSet<String>>();
		HashSet<String> setA = new HashSet<String>();
		HashSet<String> setB = new HashSet<String>();
		
		setA.add("1");
		setA.add("2");
		setA.add("3");
		setA.add("4");
		
		setB.add("5");
		
		slaves.put(1, setA);
		
		setB.addAll(slaves.get(1));
		slaves.remove(1);
		

		
		int p =1;
	}
}
