package edu.buffalo.cse562.Operators;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.Set;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.EquivalentFilters.FilterCol;
import edu.buffalo.cse562.EquivalentFilters.NewFilterCol;
import edu.buffalo.cse562.RATree.*;

public class BigDataScanOperator implements Operator {

	/* FROM */

	private File f = null;
	private BufferedReader input = null;
	private String alias = null;
	HashMap<String, Integer> inputSchema = null;
	private HashMap<String, Integer> outputSchema = null;
	String[] colNameList = null;
	Expression[] tuple = null;
	ArrayList<String> tupleCache = new ArrayList<String>();
	SqlExpression sqlExpr = null;
	NewFilterCol colFilter = null;
	int[] colMap = null;
	int[] dataTypeList = null;
	int inputSchemaSize = 0;
	Database db = null;
	DiskOrderedCursor cursor = null;
	String keyString = null;
	String dataString = null;
	Environment dbEnv = null;

	public BigDataScanOperator(Environment dbEnv, File f,
			ArrayList<ColumnDefinition> schema, String alias) {
		this.f = f;

		this.alias = alias;

		this.tuple = new Expression[schema.size()];

		/* Get Array of column Names from list of columndefinition */
		this.colNameList = this.getColumnNameArray(schema);

		/* Get map of column names for a table */
		colFilter = new NewFilterCol();
		colMap = colFilter.getCompMap(alias);

		/* Load schema into SqlExpression */
		NewSqlExpression.loadSqlExpression(schema);

		/* Retireve size of schema */
		this.inputSchemaSize = schema.size();
		this.inputSchema = new HashMap<String, Integer>();

		/*
		 * Creating Map inputschema as mapping of column name and corresponding
		 * index
		 */
		for (int i = 0; i < this.inputSchemaSize; i++) {
			this.inputSchema
					.put(schema.get(i).getColumnName().toUpperCase(), i);
		}

		/* Assign output schema */
		this.outputSchema = this.inputSchema;

		/* Enumerate each Column Data type */
		dataTypeList = this.getDataTypeList(schema);
		

		this.dbEnv = dbEnv;
		//System.out.println("Cache percent :"+this.dbEnv.getMutableConfig().getCachePercent());
		this.reset();
	}

	public String getAlias() {
		return alias;
	}

	public HashMap<String, Integer> getOutputSchema() {
		return this.outputSchema;
	}

	public Expression[] getTuple() {
		String line = null;
		Expression expr = null;
		// this.tuple = new HashMap<String, Expression>(20);
		ByteArrayInputStream byteArray = null;
		DataInputStream dataInput = null;
		this.tuple = new Expression[this.inputSchemaSize];

		int index = 0;
		int count = 1;
		int colItr = 0;
		int inputSchemaItr = 0;
		String temp = null;

		ColumnDefinition colDef = null;
		byte[] bArray =  getRecord();
		if(bArray!=null)
			byteArray = new ByteArrayInputStream(bArray);
		else{
			
			this.cursor.close();
			this.db.close();
			return null;
		}
		
		dataInput = new DataInputStream(byteArray);
		
		/*if (keyString == null) {
			this.cursor.close();
			this.db.close();
			return null;
		}*/

		colItr = 0;

		while (inputSchemaItr < this.inputSchemaSize) {
			// colDef = this.inputSchema.get(inputSchemaItr);

			if (colMap == null
					|| (colMap != null && colMap[inputSchemaItr] == 1)) {
				// System.out.println(inputSchemaItr);
				try {

					if (this.dataTypeList[inputSchemaItr] == 1){
						expr = new LongValue(dataInput.readInt());
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 2){
						StringBuilder strExpr = new StringBuilder(" ");
						strExpr.append(dataInput.readUTF());
						strExpr.append(" ");
						expr = new StringValue(strExpr.toString());
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 3){
						StringBuilder str = new StringBuilder(" ");
						str.append(dataInput.readUTF());
						str.append(" ");
						expr = new StringValue(str.toString());
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 4){
						StringBuilder strExpr = new StringBuilder(" ");
						strExpr.append(dataInput.readUTF());
						strExpr.append(" ");
						expr = new StringValue(strExpr.toString());
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 5){
						
						expr = new DoubleValue(dataInput.readDouble());
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 6){
						StringBuilder date = new StringBuilder(" ");
						date.append(dataInput.readUTF());
						date.append(" ");
						expr = new DateValue(date.toString());
						
					}

				} catch (IOException ioExpt) {
					ioExpt.printStackTrace();
				}

				/*
				 * expr = NewSqlExpression.getExpression(
				 * this.dataTypeList[inputSchemaItr], tmp[inputSchemaItr]);
				 */
				
				if (expr == null)
					return null;

				// tuple.put(this.colNameList[colItr], expr);
				tuple[inputSchemaItr] = expr;
				colItr++;
			} else {
				// tuple.put(this.colNameList[colItr], expr);
				try {
					if (this.dataTypeList[inputSchemaItr] == 1) {
						int tempInt = dataInput.readInt();
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 2) {
						String tempStr = dataInput.readUTF();
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 3) {
						String tempStr = dataInput.readUTF();
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 4) {
						String tempStr = dataInput.readUTF();
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 5) {
						double tempDouble = dataInput.readDouble();
						
					}
					else if (this.dataTypeList[inputSchemaItr] == 6) {
						String tempStr = dataInput.readUTF();
						
					}
				} catch (IOException IOExcept) {
					IOExcept.printStackTrace();
				}
				tuple[colItr] = expr;
				colItr++;
			}

			count++;
			inputSchemaItr++;
		}

		return tuple;
	}

	/*
	 * public ArrayList<String> getColumnNameList( ArrayList<ColumnDefinition>
	 * inputSchema) { Iterator<ColumnDefinition> itr = null; ArrayList<String>
	 * colNameList = null; if (inputSchema == null) return null; else itr =
	 * inputSchema.iterator();
	 * 
	 * colNameList = new ArrayList<String>();
	 * 
	 * while (itr.hasNext()) { colNameList.add(itr.next().getColumnName()); }
	 * 
	 * return colNameList; }
	 */

	public String[] getColumnNameArray(ArrayList<ColumnDefinition> inputSchema) {
		Iterator<ColumnDefinition> itr = null;
		String[] colNameList = null;
		int i = 0;

		if (inputSchema == null)
			return null;
		else
			itr = inputSchema.iterator();

		colNameList = new String[inputSchema.size()];

		while (itr.hasNext()) {
			colNameList[i] = itr.next().getColumnName();
			i++;
		}

		return colNameList;
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
		// System.out.println(inputSchema);
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

	public void reset() {
		try {
			// input = new BufferedReader(new FileReader(f));
			if (this.db != null)
				this.db.close();
			else {
				DatabaseConfig dbConfig = new DatabaseConfig();
				// dbConfig.setAllowCreate(true);
				dbConfig.setReadOnly(true);
				// dbConfig.setDeferredWrite(true);

				this.db = this.dbEnv.openDatabase(null, alias, dbConfig);
			}

			if (cursor != null)
				cursor.close();
			else {
				DiskOrderedCursorConfig config = new DiskOrderedCursorConfig();
				System.out.println("Internal memory: "+config.getInternalMemoryLimit());
				System.out.println("queue size:" + config.getQueueSize());
				System.out.println("lsn batch size:"+config.getLSNBatchSize());
				config.setQueueSize(2000);
				System.out.println("queue size:" + config.getQueueSize());
				cursor = db.openCursor(config);
				
			}
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}



	public byte[] getRecord() {

		try {
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();
			ByteArrayInputStream byteArray = null;
			// DataInputStream input = null;

			if (this.cursor.getNext(foundKey, foundData,
					LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
				keyString = new String(foundKey.getData(), "UTF-8");
				 dataString = new String(foundData.getData(), "UTF-8");
				 
				byteArray = new ByteArrayInputStream(foundData.getData());
				//System.out.println("key: " + keyString);
				return foundData.getData();
			} else {
				//System.out.println("key: " + null);
				keyString = null;
				dataString = null;
				return null;
			}
			
		} catch (DatabaseException | UnsupportedEncodingException de) {
			System.err.println("Error accessing database." + de);
		}

		return null;
	}
	/*
	 * public int getSize(int type){ switch(type){ case 1: return } }
	 */
}
