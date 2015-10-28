package edu.buffalo.cse562.Load;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.SecondaryDatabase;

import java.io.IOException;

public class SecIndexKeyCreator implements SecondaryKeyCreator {

	private EntryBinding theBinding;
	private int index = 0;

	public SecIndexKeyCreator(EntryBinding binding) {
		theBinding = binding;
	}

	public void setIndex(int i) {
		index = i;
	}

	public boolean createSecondaryKey(SecondaryDatabase secDb,
			DatabaseEntry keyEntry, DatabaseEntry dataEntry,
			DatabaseEntry resultEntry) {

		try {
			
			String line = (String) theBinding.entryToObject(dataEntry);
			
			String key = splitLine(line, index);
			
			resultEntry.setData(key.getBytes("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	public String splitLine(String line, int index) {
		int i = 0;
		int j = 0;
		int count = 0;
		String col = null;
		StringBuilder temp = new StringBuilder(line);
		while (count < index && (line.indexOf("|", j) >= 0)) {
			i = line.indexOf("|", j);

			if (count == index-1)
				col = temp.substring(j, i);

			j = i + 1;
			count++;
		}
		if(col==null)
			col = temp.substring(j, line.length());
		
		return col;
	}

}
