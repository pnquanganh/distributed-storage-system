package ce7490_master;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;


import client_master_interface.*;

public class testclass{
	public static HashMap<String, Reading_request_result> files;
	public static HashMap<Integer, HashSet<String>> slaves;
	

	
	public static void main(String[] args){
		Vector<Hierachical_codes> missing = new Vector<Hierachical_codes>();
		missing.add(Hierachical_codes.O3);
		missing.add(Hierachical_codes.O3O4);
		missing.add(Hierachical_codes.O1O2);
		
		HashMap<Hierachical_codes, HashSet<Hierachical_codes>> a = hierachical_code.recovery (missing);
		
		for (Hierachical_codes i: Hierachical_codes.values()){
			HashSet<Hierachical_codes> t = a.get(i);
			System.out.print(i);
			System.out.print(" ");
			for (Hierachical_codes c: t){
				System.out.print(c);
				System.out.print(" ");
			}
			System.out.println();
		}
		
		int p =1;
	}
}
