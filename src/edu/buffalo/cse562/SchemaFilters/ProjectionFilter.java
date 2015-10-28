package edu.buffalo.cse562.SchemaFilters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.SqlParser.TablesO;

public class ProjectionFilter extends Filter {
	ArrayList<Object> component = null;
	HashMap<String, TablesO> parsedTables = null;
	TablesO table = null;
	ArrayList<ColumnDefinition> inputSchema = null;
	ArrayList<ColumnDefinition> outputSchema = null;
	
	static
	{
		FilterFactorySingleton.getFactoryInstance()
			.getFactory().registerFilter("Projection",new ScanFilter());
	}

	
	public Filter createFilter(){
		return new ScanFilter();
	}
	
	public void loadSchema(ArrayList<Object> component,
			ArrayList<ColumnDefinition> inputSchema, 
			HashMap<String, TablesO> parsedTables){
		this.parsedTables = parsedTables;
		this.inputSchema = inputSchema;
		ColumnDefinition colDef = null;
		Iterator itr = component.iterator();
		ColDataType type = new ColDataType();
		outputSchema = new ArrayList<ColumnDefinition>();
		
		while (itr.hasNext()){	
			colDef = new ColumnDefinition();
			colDef.setColumnName(itr.next().toString());
			type.setDataType("string");
            colDef.setColDataType(type);
			outputSchema.add(colDef);
		}
		
	}
	
	public ArrayList<ColumnDefinition> getInputSchema(){
		return inputSchema;
	}
	public ArrayList<ColumnDefinition> getOutputSchema(){
		return outputSchema;
	}
}
