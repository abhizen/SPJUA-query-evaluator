package edu.buffalo.cse562.Operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class Samp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LinkedHashMap<String,Integer> hsh = new LinkedHashMap<String,Integer>();
		hsh.put("B", new Integer(1));
		hsh.put("A", new Integer(1));
		hsh.put("D", new Integer(1));
		hsh.put("C", new Integer(1));
		
		List<Map.Entry<String, Integer>> entries =
				  new ArrayList<Map.Entry<String, Integer>>(hsh.entrySet());
				Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
				  public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b){
				    return a.getKey().compareTo(b.getKey());
				  }
				});
				Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
				for (Map.Entry<String, Integer> entry : entries) {
				  sortedMap.put(entry.getKey(), entry.getValue());
				}
				
				System.out.println(sortedMap);
				
				String buf = "ABC.VARCHAR";
				if(buf.contains(".")){
					StringBuffer buff = new StringBuffer(buf);
					int index = buf.indexOf(".");
					buf = buff.substring(index+1, buf.length());
					System.out.println(buf);
				}
				
				StringTokenizer tmp = new StringTokenizer("ABC|CDE","|");
				while(tmp.hasMoreTokens()){
					System.out.println(tmp.nextToken());
				}
				
				HashMap<String,String> flagMap = new HashMap(10);  
				flagMap.put("A","B");
				System.out.println(flagMap.get("A"));
				
				StringBuilder tmp1 = new StringBuilder("ABC|CDE|PDF|MNE");
				System.out.println("-------------------");
				tmp1.indexOf("|");
				int i = 0;
				int j = 0;
				int count = 0;
				int index = 0;
				String substr = null;
				while(tmp1.indexOf("|", i)>=0){
					index=  tmp1.indexOf("|", i);
					j = index;
					//System.out.println(index);
					substr = tmp1.substring(i, j);
					System.out.println(substr);
					i = j+1;
					count++;
				}
				
				substr = tmp1.substring(i, tmp1.length());
				System.out.println(substr);
				
				int[] a = {1,2,3,4};
				int [] b = a.clone();
				System.out.println(a[0]);
				System.out.println(b[2]);
				b[2] = 10;
				System.out.println(a[2]);
				
	}

}
