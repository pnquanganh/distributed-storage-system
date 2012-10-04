/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ce7490_master;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import Info.Hierachical_codes;

public class hierachical {
	public static HashMap<Hierachical_codes, HashSet<Hierachical_codes>> recovery (Vector<Hierachical_codes> missing){
		HashMap<Hierachical_codes, HashSet<Hierachical_codes>> recovered = new HashMap<Hierachical_codes, HashSet<Hierachical_codes>>();
		
		for (Hierachical_codes i: Hierachical_codes.values()){
			if (!missing.contains(i)){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.add(i);
				recovered.put(i, temp);
			}
		}
		
		HashSet<Hierachical_codes> a;
		HashSet<Hierachical_codes> b;
		
		if (missing.contains(Hierachical_codes.O1O2)){
			a = recovered.get(Hierachical_codes.O1);
			b = recovered.get(Hierachical_codes.O2);
			
			if (a!=null && b!= null){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.addAll(a);
				temp.addAll(b);
				
				recovered.put(Hierachical_codes.O1O2, temp);
				missing.remove(Hierachical_codes.O1O2);
			}
		}
		if (missing.contains(Hierachical_codes.O3O4)){
			a = recovered.get(Hierachical_codes.O3);
			b = recovered.get(Hierachical_codes.O4);
			
			if (a!=null && b!= null){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.addAll(a);
				temp.addAll(b);
				
				recovered.put(Hierachical_codes.O3O4, temp);
				missing.remove(Hierachical_codes.O3O4);
			}
		}
		if (missing.contains(Hierachical_codes.O1O2O3O4)){
			a = recovered.get(Hierachical_codes.O1O2);
			b = recovered.get(Hierachical_codes.O3O4);
			
			if (a!=null && b!= null){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.addAll(a);
				temp.addAll(b);
				
				recovered.put(Hierachical_codes.O1O2O3O4, temp);
				missing.remove(Hierachical_codes.O1O2O3O4);
			}
		}
		if (missing.contains(Hierachical_codes.O1O2)){
			a = recovered.get(Hierachical_codes.O1O2O3O4);
			b = recovered.get(Hierachical_codes.O3O4);
			
			if (a!=null && b!= null){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.addAll(a);
				temp.addAll(b);
				
				recovered.put(Hierachical_codes.O1O2, temp);
				missing.remove(Hierachical_codes.O1O2);
			}
		}
		if (missing.contains(Hierachical_codes.O3O4)){
			a = recovered.get(Hierachical_codes.O1O2O3O4);
			b = recovered.get(Hierachical_codes.O1O2);
			
			if (a!=null && b!= null){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.addAll(a);
				temp.addAll(b);
				
				recovered.put(Hierachical_codes.O3O4, temp);
				missing.remove(Hierachical_codes.O3O4);
			}
		}
		if (missing.contains(Hierachical_codes.O1)){
			a = recovered.get(Hierachical_codes.O1O2);
			b = recovered.get(Hierachical_codes.O2);
			
			if (a!=null && b!= null){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.addAll(a);
				temp.addAll(b);
				
				recovered.put(Hierachical_codes.O1, temp);
				missing.remove(Hierachical_codes.O1);
			}
		}
		if (missing.contains(Hierachical_codes.O2)){
			a = recovered.get(Hierachical_codes.O1O2);
			b = recovered.get(Hierachical_codes.O1);
			
			if (a!=null && b!= null){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.addAll(a);
				temp.addAll(b);
				
				recovered.put(Hierachical_codes.O2, temp);
				missing.remove(Hierachical_codes.O2);
			}
		}
		if (missing.contains(Hierachical_codes.O3)){
			a = recovered.get(Hierachical_codes.O3O4);
			b = recovered.get(Hierachical_codes.O4);
			
			if (a!=null && b!= null){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.addAll(a);
				temp.addAll(b);
				
				recovered.put(Hierachical_codes.O3, temp);
				missing.remove(Hierachical_codes.O3);
			}
		}
		if (missing.contains(Hierachical_codes.O4)){
			a = recovered.get(Hierachical_codes.O3O4);
			b = recovered.get(Hierachical_codes.O3);
			
			if (a!=null && b!= null){
				HashSet<Hierachical_codes> temp = new HashSet<Hierachical_codes>();
				temp.addAll(a);
				temp.addAll(b);
				
				recovered.put(Hierachical_codes.O4, temp);
				missing.remove(Hierachical_codes.O4);
			}
		}
		
		return recovered;
	}
 
}
