package edu.buffalo.cse562.Load;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Test {
	public static void main(String []args){
		String line = "133|7|5|2|12|12926.04|0.02|0.06|N|O|1997-12-02|1998-01-15";
		int i = 0;
		int j = 0;
		int count = 0;
		String col = null;
		StringBuilder temp = new StringBuilder(line);
		int index = 12;
		while (count < index && (line.indexOf("|", j) >= 0)) {
			i = line.indexOf("|", j);

			if (count == index-1)
				col = temp.substring(j, i);

			j = i + 1;
			count++;
		}
		if(col==null)
			col = temp.substring(j, line.length());
		
		System.out.println(col);
		line = "1|5|4|1|17|17954.55|0.04|0.02|N|O|1996-03-13|1996-02-12";
		for(int z=0;z<2;z++){
		String[] col1 = line.split("\\|");
		System.out.println(line);
		ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(byteArray);
		String token = null;
		 i =0;
		 count = 0;
		while(line.indexOf("|",i)>=0){
			try{
				if(count<8)
					out.writeDouble(Double.parseDouble(col1[count]));
				else
					out.writeUTF(col1[count]);
					
				
			}catch(Exception e){
				e.printStackTrace();
			}
			j = line.indexOf("|",i);
			i = j+1;
			count++;
		}
		i = 0;
		byte[] b =byteArray.toByteArray();
		ByteArrayInputStream byteInputArray = new ByteArrayInputStream(b);
		DataInputStream d = new DataInputStream(byteInputArray);
		 count = 0;
		 i = 0;
		while(line.indexOf("|",i)>=0){
			try{
				if(count<8)	{
					double temp1 = d.readDouble();
					System.out.println(temp1);
				}else{
					String temp1 = d.readUTF();
					System.out.println(temp1);
				}
			}
			catch(IOException e){
				e.printStackTrace();
			}
			j = line.indexOf("|",i);
			i = j+1;
			count++;
		}
	}
	}
}
