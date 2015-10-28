package edu.buffalo.cse562.EquivalentFilters;

import java.util.HashMap;

public class FilterCol {
	HashMap<Integer,Integer> qCompMap = null;
	public static String query = null;

	public static void setQuery(String queryFile){
		query =queryFile;
	}

	public HashMap<Integer,Integer> getCompMap(String table){
		Integer dummy = new Integer("1");
		
		if(query.contains("1.sql") && table.equals("LINEITEM")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(5), dummy);
			qMap.put(new Integer(6), dummy);
			qMap.put(new Integer(7), dummy);
			qMap.put(new Integer(8), dummy);
			qMap.put(new Integer(9), dummy);
			qMap.put(new Integer(10), dummy);
			qMap.put(new Integer(11), dummy);
			return qMap; 
		}
		
		if(query.contains("3.sql") && table.equals("LINEITEM")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(6), dummy);
			qMap.put(new Integer(7), dummy);
			qMap.put(new Integer(11), dummy);
			return qMap; 
		}
		
		if(query.contains("3.sql") && table.equals("ORDERS")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(2), dummy);
			qMap.put(new Integer(5), dummy);
			qMap.put(new Integer(8), dummy);
			
			return qMap; 
		}
		
		if(query.contains("3.sql") && table.equals("CUSTOMER")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(7), dummy);
			
			return qMap; 
		}
		
		if(query.contains("5.sql") && table.equals("LINEITEM")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(3), dummy);
			qMap.put(new Integer(6), dummy);
			qMap.put(new Integer(7), dummy);
			return qMap; 
		}
		
		
		if(query.contains("5.sql") && table.equals("CUSTOMER")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			
			return qMap; 
		}
		
		if(query.contains("5.sql") && table.equals("ORDERS")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(2), dummy);
			qMap.put(new Integer(5), dummy);
			
			return qMap; 
		}
		
		if(query.contains("5.sql") && table.equals("CUSTOMER")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(7), dummy);
			
			return qMap; 
		}
		
		if(query.contains("5.sql") && table.equals("REGION")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(2), dummy);
			
			return qMap; 
		}
		
		
		if(query.contains("5.sql") && table.equals("NATION")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(2), dummy);
			qMap.put(new Integer(3), dummy);
			
			return qMap; 
		}
		

		if(query.contains("5.sql") && table.equals("SUPPLIER")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(4), dummy);
		
			return qMap; 
		}
		
		if(query.contains("12.sql") && table.equals("LINEITEM")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(	11), dummy);
			qMap.put(new Integer(12), dummy);
			qMap.put(new Integer(13), dummy);
			qMap.put(new Integer(15), dummy);
			
			return qMap; 
		}
		
		if(query.contains("12.sql") && table.equals("ORDERS")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(6), dummy);
			
			return qMap; 
		}
		
		if(query.contains("10.sql") && table.equals("LINEITEM")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(6), dummy);
			qMap.put(new Integer(7), dummy);
			qMap.put(new Integer(9), dummy);
			return qMap; 
		}
		
		if(query.contains("10.sql") && table.equals("ORDERS")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(2), dummy);
			qMap.put(new Integer(5), dummy);
			return qMap; 
		}
		
		if(query.contains("10.sql") && table.equals("CUSTOMER")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(3), dummy);
			qMap.put(new Integer(4), dummy);
			qMap.put(new Integer(5), dummy);
			qMap.put(new Integer(6), dummy);
			qMap.put(new Integer(8), dummy);
			return qMap; 
		}
		
		if(query.contains("10.sql") && table.equals("NATION")){
			HashMap<Integer,Integer> qMap = new HashMap<>();
			qMap.put(new Integer(1), dummy);
			qMap.put(new Integer(2), dummy);
			
			return qMap; 
		}
		
		return null;
	}
}
