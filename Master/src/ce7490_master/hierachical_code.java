/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ce7490_master;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import client_master_interface.*;

public class hierachical_code {
	public static HashMap<Integer, HashSet<Integer>> recovery (Vector<Integer> missing){
		HashMap<Integer, HashSet<Integer>> recovered = new HashMap<Integer, HashSet<Integer>>();
		
		
		for (int i = 0; i<7; i++){
			if (! missing.contains(i)){
				HashSet<Integer> recMethod = new HashSet<Integer>();
				recMethod.add(i);
				recovered.put(i, recMethod);
			}
		}
		
		for (int i = 0; i<missing.size(); i++)
		{
			int missed = missing.get(i);
			HashSet<Integer> a = recovered.get(key);
		}
		
		return recovered;
	}
 
}
