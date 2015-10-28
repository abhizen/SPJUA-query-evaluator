package edu.buffalo.cse562.Load;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class LoadPhase {

	Database myDatabase = null;
	File path = null;
	Environment dbEnv = null;
	private HashMap<String, SecondaryDatabase> secDbMap = new HashMap<>();
	private ArrayList<ColumnDefinition> schema = null;
	private int [] colIndex = null; 
	public LoadPhase(Database myDatabase, File path, Environment env,ArrayList<ColumnDefinition> schema) {
		this.myDatabase = myDatabase;
		this.path = path;
		this.schema = schema;
		dbEnv = env;
		colIndex = getDataTypeList(schema);
	}
	
	public HashMap<String, SecondaryDatabase> getSecDbMap(){
		return secDbMap;
	}
	public Database processLineItem() {
		// Only difference for the primary key, using seq num
		String line = null;
		String[] col = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		SecondaryDatabase mySecDb = null;
		Integer seqNum = 0;
		ByteArrayOutputStream dataOut = null;
		
		// SecondaryConfig mySecConfig = new SecondaryConfig();

		/* Secondary Index */
		EntryBinding myBinding = TupleBinding.getPrimitiveBinding(String.class);

		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {

				key = seqNum.toString();
				
				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				
				dataOut = this.splitLine(line);
				
				theData = new DatabaseEntry(dataOut.toByteArray());
				//myBinding.objectToEntry(line, theData);

				myDatabase.put(null, theKey, theData);
				
				seqNum++;
			}

			/*
			 * mySecConfig.setAllowCreate(true);
			 * mySecConfig.setSortedDuplicates(true); // Open the secondary. //
			 * Key creators are described in the next section.
			 * SecIndexKeyCreator keyCreator = new
			 * SecIndexKeyCreator(myBinding); keyCreator.setIndex(4);
			 * 
			 * // Get a secondary object and set the key creator on it.
			 * mySecConfig.setKeyCreator(keyCreator);
			 * mySecConfig.setAllowPopulate(true);
			 * 
			 * // Perform the actual open String secDbName =
			 * "mySecondaryDatabase"; mySecDb =
			 * dbEnv.openSecondaryDatabase(null, secDbName, myDatabase,
			 * mySecConfig);
			 */
			/*mySecDb = createSecDb(myBinding, myDatabase, 11,
					"LINEITEM.SHIPMODE");*/
			//getRecord(mySecDb, "8");
			/*secDbMap.put("LINEITEM.SHIPMODE", mySecDb);*/

		} catch (IOException e) {
			e.printStackTrace();

		}
		//mySecDb.close();
		// getAllRecord();
		return myDatabase;
	}

	public Database processOrders() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		SecondaryDatabase mySecDb = null;
		ByteArrayOutputStream dataOut = null;
		EntryBinding myBinding = TupleBinding.getPrimitiveBinding(String.class);
		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				
				dataOut = this.splitLine(line);
				theData = new DatabaseEntry(dataOut.toByteArray());
				
				//myBinding.objectToEntry(line, theData);
				
				myDatabase.put(null, theKey, theData);

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		/*mySecDb = createSecDb(myBinding, myDatabase, 5, "ORDERS.ORDERDATE");
		//getRecord(mySecDb, "8");
		secDbMap.put("ORDERS.ORDERDATE", mySecDb);
		mySecDb.close();*/
		
		// getAllRecord();
		return myDatabase;
	}

	public Database processCustomer() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		EntryBinding myBinding = TupleBinding.getPrimitiveBinding(String.class);
		SecondaryDatabase mySecDb = null;
		ByteArrayOutputStream dataOut = null;

		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				dataOut = this.splitLine(line);
				theData = new DatabaseEntry(dataOut.toByteArray());
				
				//myBinding.objectToEntry(line, theData);
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		/*mySecDb = createSecDb(myBinding, myDatabase, 7, "CUSTOMER.MKTSEGMENT");
		//getRecord(mySecDb, "8");
		secDbMap.put("CUSTOMER.MKTSEGMENT", mySecDb);
		mySecDb.close();*/
		// getAllRecord();
		return myDatabase;
	}

	public Database processSupplier() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		ByteArrayOutputStream dataOut = null;

		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				dataOut = this.splitLine(line);
				theData = new DatabaseEntry(dataOut.toByteArray());
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();

		}
		// getAllRecord();
		return myDatabase;
	}

	public Database processPartSupp() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		ByteArrayOutputStream dataOut= null;
		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				dataOut = this.splitLine(line);
				theData = new DatabaseEntry(dataOut.toByteArray());
				
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();

		}
		// getAllRecord();
		return myDatabase;
	}

	public Database processNation() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		ByteArrayOutputStream dataOut = null;
		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				dataOut = this.splitLine(line);
				theData = new DatabaseEntry(dataOut.toByteArray());
				
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();

		}
		// getAllRecord();
		return myDatabase;
	}

	public Database processRegion() {
		String line = null;
		int index;
		String key;
		DatabaseEntry theKey = null;
		DatabaseEntry theData = null;
		ByteArrayOutputStream dataOut = null;
		
		try {
			BufferedReader input = new BufferedReader(new FileReader(path));
			while ((line = input.readLine()) != null) {
				// line = input.readLine();
				index = line.indexOf("|");
				key = line.substring(0, index);

				theKey = new DatabaseEntry(key.getBytes("UTF-8"));
				dataOut = this.splitLine(line);
				theData = new DatabaseEntry(dataOut.toByteArray());
				myDatabase.put(null, theKey, theData);
			}
		} catch (IOException e) {
			e.printStackTrace();

		}
		// getAllRecord();
		return myDatabase;
	}

	public void getAllRecord() {
		Cursor cursor = null;
		try {

			cursor = myDatabase.openCursor(null, null);

			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();

			while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

				String keyString = new String(foundKey.getData(), "UTF-8");
				String dataString = new String(foundData.getData(), "UTF-8");

			}
		} catch (DatabaseException | UnsupportedEncodingException de) {
			System.err.println("Error accessing database." + de);
		} finally {
			// Cursors must be closed.
			cursor.close();
		}
	}

	public void getRecord(SecondaryDatabase mySecDb, String aKey) {

		Cursor cursor = null;
		try {

			// Database and environment open omitted for brevity

			// Open the cursor.
			cursor = mySecDb.openCursor(null, null);

			// Cursors need a pair of DatabaseEntry objects to operate. These
			// hold
			// the key and data found at any given position in the database.
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();

			// To iterate, just call getNext() until the last database record
			// has
			// been read. All cursor operations return an OperationStatus, so
			// just
			// read until we no longer see OperationStatus.SUCCESS
			while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				// getData() on the DatabaseEntry objects returns the byte array
				// held by that object. We use this to get a String value. If
				// the
				// DatabaseEntry held a byte array representation of some other
				// data type (such as a complex object) then this operation
				// would
				// look considerably different.
				String keyString = new String(foundKey.getData(), "UTF-8");
				String dataString = new String(foundData.getData(), "UTF-8");
				
			}
		} catch (DatabaseException | UnsupportedEncodingException de) {
			System.err.println("Error accessing database." + de);
		} finally {
			// Cursors must be closed.
			cursor.close();
		}
	}

	public SecondaryDatabase createSecDb(EntryBinding binding,
			Database myDatabase, int index, String name) {
		SecondaryConfig mySecConfig = new SecondaryConfig();
		mySecConfig.setAllowCreate(true);
		mySecConfig.setSortedDuplicates(true);
		// Open the secondary.
		// Key creators are described in the next section.
		SecIndexKeyCreator keyCreator = new SecIndexKeyCreator(binding);
		keyCreator.setIndex(index);

		// Get a secondary object and set the key creator on it.
		mySecConfig.setKeyCreator(keyCreator);
		mySecConfig.setAllowPopulate(true);

		// Perform the actual open
		String secDbName = name;
		SecondaryDatabase mySecDb = dbEnv.openSecondaryDatabase(null,
				secDbName, myDatabase, mySecConfig);
		return mySecDb;
	}
	public ByteArrayOutputStream splitLine(String line){
		int i = 0;
		int j = 0;
		int count = 0;
		int index = 0;
		String substr = null;
		StringBuilder tmp1 = new StringBuilder(line);
		int inputSchemaSize = this.schema.size();
		String[] tmp =new String[inputSchemaSize];
		int len = 0;
		int p = 0;
		ColumnDefinition colDef = null;
		ByteArrayOutputStream out  = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out); 

		while(count<inputSchemaSize && tmp1.indexOf("|", i)>=0){
			index=  tmp1.indexOf("|", i);
			j = index;
			
			colDef = this.schema.get(p);
			
			tmp[count] =tmp1.substring(i, j);
			
			try{
				if(colIndex[p] == 1)
					dataOut.writeInt(Integer.parseInt(tmp[count]));
				else if(colIndex[p] == 2)
					dataOut.writeUTF(tmp[count]);
				else if(colIndex[p] == 3)
					dataOut.writeUTF(tmp[count]);
				else if(colIndex[p] == 4)
					dataOut.writeUTF(tmp[count]);
				else if(colIndex[p] == 5)
					dataOut.writeDouble(Double.parseDouble(tmp[count]));
				else if(colIndex[p] == 6)
					dataOut.writeUTF(tmp[count]);
		    	}catch(IOException e){
		    		e.printStackTrace();
		    	}
				
			i = j+1;
			count++;
			p++;
		}
		len = line.length();

		if(count<inputSchemaSize){
			tmp[count] = tmp1.substring(i, len);
			try{
				if(colIndex[p] == 1)
					dataOut.writeInt(Integer.parseInt(tmp[count]));
				else if(colIndex[p] == 2)
					dataOut.writeUTF(tmp[count]);
				else if(colIndex[p] == 3)
					dataOut.writeUTF(tmp[count]);
				else if(colIndex[p] == 4)
					dataOut.writeUTF(tmp[count]);
				else if(colIndex[p] == 5)
					dataOut.writeDouble(Double.parseDouble(tmp[count]));
				else if(colIndex[p] == 6)
					dataOut.writeUTF(tmp[count]);
		    	}catch(IOException e){
		    		e.printStackTrace();
		    	}
		}
		
		try{
			dataOut.close();
		}catch(IOException e1){
			e1.printStackTrace();
		}
		
		return out;
	}
	
	public int[] getDataTypeList(ArrayList<ColumnDefinition> inputSchema) {
		Iterator<ColumnDefinition> itr = null;
		int[] dataTypeList = null;
		ColumnDefinition colDef = null;
		String colDataType = null;
		int indexItr = 0;

		if (inputSchema == null)
			return null;
		else
			itr = inputSchema.iterator();
                //System.out.println(inputSchema);
		dataTypeList = new int[inputSchema.size()];

		while (itr.hasNext()) {

			colDef = itr.next();
			colDataType = colDef.getColDataType().getDataType();

			if (colDataType.contains("(")) {
				int index = colDataType.indexOf("(");
				StringBuffer colType = new StringBuffer(colDataType);
				colDataType = colType.substring(0, index).toUpperCase();
				colType = null;
			} else
				colDataType = colDataType.toUpperCase();

			if (colDataType.equals("INT"))
				dataTypeList[indexItr] = 1;
			else if (colDataType.equals("VARCHAR"))
				dataTypeList[indexItr] = 2;
			else if (colDataType.equals("CHAR"))
				dataTypeList[indexItr] = 3;
			else if (colDataType.equals("STRING"))
				dataTypeList[indexItr] = 4;
			else if (colDataType.equals("DECIMAL"))
				dataTypeList[indexItr] = 5;
			else if (colDataType.equals("DATE"))
				dataTypeList[indexItr] = 6;

			indexItr++;
		}

		return dataTypeList;
	}

	
}
