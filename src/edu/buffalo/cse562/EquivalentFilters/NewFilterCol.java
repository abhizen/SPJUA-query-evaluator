package edu.buffalo.cse562.EquivalentFilters;

import java.util.HashMap;

public class NewFilterCol {
HashMap<Integer,Integer> qCompMap = null;
public static String query = null;

	public static void setQuery(String queryFile){
		query =queryFile;
	}

	public int[] getCompMap(String table){
		Integer dummy = new Integer("1");
		
		if(query.contains("1.sql") && table.equals("LINEITEM")){
			
			int[] qList =  new int[16];
			for(int i =0;i<16;i++){
				qList[i] = 0;
			}
			qList[3] =1;
			qList[4] =1;
			qList[5] =1;
			qList[6] =1;
			qList[7] =1;
			qList[8] =1;
			qList[9] =1;
			qList[10] =1;
			return qList; 
		}
		
		if(query.contains("3.sql") && table.equals("LINEITEM")){
			int[] qList =  new int[16];
			for(int i =0;i<16;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[5] =1;
			qList[6] =1;
			qList[10] =1;
			return qList; 
		}
		
		if(query.contains("3.sql") && table.equals("ORDERS")){
			
			int[] qList =  new int[9];
			for(int i =0;i<9;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[1] =1;
			qList[4] =1;
			qList[7] =1;
			return qList;
		}
		
		if(query.contains("3.sql") && table.equals("CUSTOMER")){
			
			int[] qList =  new int[8];
			for(int i =0;i<8;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[6] =1;
			return qList;
			 
		}
		
		if(query.contains("5.sql") && table.equals("LINEITEM")){
			int[] qList =  new int[16];
			for(int i =0;i<8;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[2] =1;
			qList[5] =1;
			qList[6] =1;
			return qList;
			 
		}
		
		
		
		if(query.contains("5.sql") && table.equals("ORDERS")){
			int[] qList =  new int[9];
			for(int i =0;i<9;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[1] =1;
			qList[4] =1;
			return qList; 
		}
		
		if(query.contains("5.sql") && table.equals("CUSTOMER")){
			int[] qList =  new int[8];
			for(int i =0;i<8;i++){
				qList[i] = 0;
			}
			
			qList[0] =1;
			qList[3] =1;
			qList[6] =1;
			return qList; 
			
		}
		
		if(query.contains("5.sql") && table.equals("REGION")){
			
			int[] qList =  new int[3];
			for(int i =0;i<3;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[1] =1;
			return qList;
		}
		
		
		if(query.contains("5.sql") && table.equals("NATION")){
			
			int[] qList =  new int[4];
			for(int i =0;i<3;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[1] =1;
			qList[2] =1;
			return qList;
		}
		

		if(query.contains("5.sql") && table.equals("SUPPLIER")){
			int[] qList =  new int[7];
			for(int i =0;i<7;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[3] =1;
			return qList; 
		}
		
		if(query.contains("6.sql") && table.equals("LINEITEM")){
			int[] qList =  new int[16];
			for(int i =0;i<7;i++){
				qList[i] = 0;
			}
			qList[4] =1;
			qList[5] =1;
			qList[6] =1;
			qList[10] =1;
			return qList; 
		}
		
		if(query.contains("12.sql") && table.equals("LINEITEM")){
			
			
			int[] qList =  new int[16];
			for(int i =0;i<16;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[10] =1;
			qList[11] =1;
			qList[12] =1;
			qList[14] =1;
			return qList;
 
		}
		
		if(query.contains("12.sql") && table.equals("ORDERS")){
			
			int[] qList =  new int[9];
			for(int i =0;i<9;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[5] =1;
		
			return qList; 
		}
		
		if(query.contains("10.sql") && table.equals("LINEITEM")){
			
			
			int[] qList =  new int[16];
			for(int i =0;i<16;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[5] =1;
			qList[6] =1;
			qList[8] =1;
			
			return qList; 
		}
		
		if(query.contains("10.sql") && table.equals("ORDERS")){
			
			int[] qList =  new int[9];
			for(int i =0;i<9;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[1] =1;
			qList[4] =1;
		
			return qList; 
		}
		
		if(query.contains("10.sql") && table.equals("CUSTOMER")){
			
			int[] qList =  new int[8];
			for(int i =0;i<8;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[2] =1;
			qList[3] =1;
			qList[4] =1;
			qList[5] =1;
			qList[7] =1;
			
			return qList; 
		}
		
		if(query.contains("10.sql") && table.equals("NATION")){
		
			int[] qList =  new int[4];
			for(int i =0;i<3;i++){
				qList[i] = 0;
			}
			qList[0] =1;
			qList[1] =1;
	
			return qList; 
		}
		
		return null;
	}
}
