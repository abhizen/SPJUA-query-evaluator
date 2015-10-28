package edu.buffalo.cse562.SchemaFilters;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.FromItem;
import edu.buffalo.cse562.SqlParser.TablesO;

public class ScanFilter extends Filter {
	ArrayList<Object> component = null;
	HashMap<String, TablesO> parsedTables = null;
	TablesO table = null;
	ArrayList<ColumnDefinition> inputSchema = null;
	ArrayList<ColumnDefinition> outputSchema = null;
	
	static
	{
		FilterFactorySingleton.getFactoryInstance()
			.getFactory().registerFilter("Scan",new ScanFilter());
	}

	
	public Filter createFilter(){
		return new ScanFilter();
	}
	
	public void loadSchema(ArrayList<Object> component, 
			HashMap<String, TablesO> parsedTables){
		this.parsedTables = parsedTables;
		String alias=null;
		String compTable = component.get(0).toString();
		FromItem from=(FromItem)component.get(0);
        
        if(from.getAlias()!=null){
       	 
            alias=from.getAlias();
            
            table=parsedTables.get(from.toString().substring(0, from.toString().indexOf(' ')));
            if(table==null){
                table=parsedTables.get(from.toString().substring(0, from.toString().indexOf(' ')).toUpperCase());
            }
            inputSchema = table.getSchema();
			outputSchema = inputSchema;
        }
        else if (from.getAlias()==null){
          
            table=parsedTables.get(from.toString().toUpperCase());
            alias=table.getTableName();
            inputSchema = table.getSchema();
			outputSchema = inputSchema;
        }
        else{
    		inputSchema = null;
			outputSchema = null;
        }
        
		/*if((table = parsedTables.get(compTable))!=null){
			inputSchema = table.getSchema();
			outputSchema = inputSchema;
		}
		else{
			inputSchema = null;
			outputSchema = null;
		}*/
			
	}
	
	public ArrayList<ColumnDefinition> getInputSchema(){
		return inputSchema;
	}
	public ArrayList<ColumnDefinition> getOutputSchema(){
		return outputSchema;
	}
}
